package com.rolandb;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rolandb.RestApiClient.RateLimitException;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.observables.ConnectableObservable;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * This class is handling the logic of polling the REST endpoint and
 * deduplicating events. It also takes care of responding to rate limiting
 * exceptions and delaying further requests adoringly.
 */
public class EventPollService {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventPollService.class);
    /**
     * Keep at most {@code MAX_PROCESSED_EVENTS} events in the deduplication set.
     * The assumption is that events will only be present for a short amount of
     * time and then be removed and never seen again.
     */
    private static final int MAX_PROCESSED_EVENTS = 6_000;

    private final ConnectableObservable<GithubEvent> observable;
    private Disposable disposable;

    /**
     * This set contains the recently processed event ids. We wrap it in a
     * {@code synchronizedSet} so that it is thread safe. We make the inner part
     * a map instead of a set directly so that we can use access order instead of
     * insertion order and evict old ids.
     */
    private final Set<String> processedEvents = Collections
            .synchronizedSet(Collections.newSetFromMap(new LinkedHashMap<>(MAX_PROCESSED_EVENTS, .75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    // Evict the oldest entry if we have more entries that the maximum.
                    return size() > MAX_PROCESSED_EVENTS;
                }
            }));

    /**
     * Create a new instance of the event polling service.
     *
     * @param apiClient
     *            The API client to poll from.
     * @param pollingIntervalMs
     *            The time interval in milliseconds between polling.
     * @param pollingDepth
     *            The number of events to query each time. This should
     *            be 300 to get all available events. May be reduced
     *            for testing at lower throughput.
     */
    public EventPollService(RestApiClient apiClient, int pollingIntervalMs, int pollingDepth) {
        // Create the observable that will emit the events.
        Scheduler ioScheduler = Schedulers.from(Executors.newFixedThreadPool(8));
        Observable<GithubEvent> coldObservable = Observable.interval(pollingIntervalMs, TimeUnit.MILLISECONDS)
                .observeOn(ioScheduler)
                .flatMapSingle(tick -> {
                    Single<List<GithubEvent>> page1 = Single
                            .fromCallable(() -> apiClient.getEvents(1, pollingDepth / 3));
                    Single<List<GithubEvent>> page2 = Single
                            .fromCallable(() -> apiClient.getEvents(2, pollingDepth / 3));
                    Single<List<GithubEvent>> page3 = Single
                            .fromCallable(() -> apiClient.getEvents(3, pollingDepth / 3));
                    return Single.zip(page1, page2, page3, (list1, list2, list3) -> {
                        LOGGER.info("Successfully fetched data from pages 1, 2, and 3");
                        return Observable.fromIterable(list1)
                                .mergeWith(Observable.fromIterable(list2))
                                .mergeWith(Observable.fromIterable(list3))
                                .toList()
                                .blockingGet();
                    });
                })
                // If an exception occurs, this we catch it and control the retry based on the
                // exception info.
                .retryWhen(errors -> errors.flatMap(ex -> {
                    if (ex instanceof RateLimitException) {
                        RateLimitException rateLimitException = (RateLimitException) ex;
                        Instant retryAfter = rateLimitException.getRetryAfter();
                        long delaySeconds = ChronoUnit.SECONDS.between(Instant.now(), retryAfter);
                        if (delaySeconds < 0) {
                            delaySeconds = 0;
                        }
                        LOGGER.warn("Rate limit exceeded. Retrying in {} seconds, at {}", delaySeconds, retryAfter);
                        return Observable.timer(delaySeconds, TimeUnit.SECONDS);
                    } else {
                        LOGGER.error("An error occurred during polling. Retrying in 60 seconds", ex);
                        return Observable.timer(60, TimeUnit.SECONDS);
                    }
                }))
                .observeOn(Schedulers.computation())
                .flatMapIterable(this::filterNewEvents);
        // Convert the cold observable to a hot one.
        this.observable = coldObservable.publish();
    }

    /**
     * Returns an Observable that once polling has been started, will emit a
     * filtered stream of new events.
     *
     * @return An Observable of new {@code GithubEvent} objects.
     */
    public Observable<GithubEvent> getEventsStream() {
        return observable;
    }

    /**
     * Starts the polling process by connecting the observable. This will start
     * the process and generate events.
     */
    public void startPolling() {
        LOGGER.info("Starting polling service");
        disposable = observable.connect();
    }

    /**
     * Stops the polling process by disconnecting the observable.
     */
    public void stopPolling() {
        if (disposable != null && !disposable.isDisposed()) {
            disposable.dispose();
            LOGGER.info("Polling service stopped");
        }
    }

    /**
     * Mark the given event as NOT processed. This may be called in case the
     * event could not be handled by a downstream consumer. In these cases, we
     * may then retry to process it later.
     *
     * @param event
     *            The event to unmark.
     */
    public void unmarkEvent(GithubEvent event) {
        processedEvents.remove(event.getId());
    }

    /**
     * Filters a list of events to find those that have not yet been processed.
     * After being returned from this method, the events will be marked as
     * processed.
     *
     * @param events
     *            The list of events from the REST endpoint.
     * @return A list of new (not yet processed) events.
     */
    private List<GithubEvent> filterNewEvents(List<GithubEvent> events) {
        return events.stream()
                .filter(event -> processedEvents.add(event.getId()))
                .toList();
    }
}
