package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.rolandb.AbstractUpdateTable;
import com.rolandb.UpdateDeduplicate;
import com.rolandb.AbstractUpdateTable.UpdateSeqRow;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * A table for collecting information about repositories. Not all events contain
 * all of the information for a given repository. This means it is necessary to
 * aggregate over all events to find as much information as possible about a
 * given repository.
 */
public class RepositoriesTable extends AbstractUpdateTable<RepositoriesTable.RepoUpdateEvent> {
    /** Type of event for this table. */
    public static class RepoUpdateEvent extends UpdateSeqRow {
        /** The id of the repository. */
        @TableEventKey
        @JsonProperty("id")
        public long id;
        /** The simple name of the repository. */
        @JsonProperty("reponame")
        public String reponame;
        /** The full name of the repository. */
        @JsonProperty("fullname")
        public String fullname;
        /** The user id of the owner. */
        @JsonProperty("owner_id")
        public Long ownerId;
        /** The GitHub HTML URL for the repository. */
        @JsonProperty("html_url")
        public String htmlUrl;
        /** The homepage for the repository. */
        @JsonProperty("homepage")
        public String homepage;
        /** The description of the repository. */
        @JsonProperty("descr")
        public String description;
        /** The space-separated list of topics. */
        @JsonProperty("topics")
        public String topics;
        /** The language in which the repository has been written. */
        @JsonProperty("lang")
        public String lang;
        /** The license that the repository has. */
        @JsonProperty("license")
        public String license;
        /** Whether or not this repository is a fork. */
        @JsonProperty("is_fork")
        public Boolean isFork;
        /** Whether or not this repository is archived. */
        @JsonProperty("is_archived")
        public Boolean isArchive;
        /** The number of forks this repository has. */
        @JsonProperty("fork_count")
        public Long forkCount;
        /** The number of issues this repository has. */
        @JsonProperty("issue_count")
        public Long issueCount;
        /** The number of starts this repository has. */
        @JsonProperty("star_count")
        public Long starCount;

        private RepoUpdateEvent(
                long id, String reponame, String fullname, Long ownerId, String htmlUrl, String homepage,
                String description, String topics, String lang, String license, Boolean isFork, Boolean isArchive,
                Long forkCount, Long issueCount, Long starCount) {
            this.id = id;
            this.reponame = reponame;
            this.fullname = fullname;
            this.ownerId = ownerId;
            this.htmlUrl = htmlUrl;
            this.homepage = homepage;
            this.description = description;
            this.topics = topics;
            this.lang = lang;
            this.license = license;
            this.isFork = isFork;
            this.isArchive = isArchive;
            this.forkCount = forkCount;
            this.issueCount = issueCount;
            this.starCount = starCount;
        }

        private static void readFromRepoObject(JsonNode rawRepo, List<RepoUpdateEvent> events) {
            if (rawRepo.isObject() && rawRepo.has("id")) {
                long id = rawRepo.at("/id").asLong();
                String reponame = null;
                String fullname = null;
                Long ownerId = null;
                String htmlUrl = null;
                String homepage = null;
                String description = null;
                String topics = null;
                String lang = null;
                String license = null;
                Boolean isFork = null;
                Boolean isArchive = null;
                Long forkCount = null;
                Long issueCount = null;
                Long starCount = null;
                if (rawRepo.has("name")) {
                    reponame = rawRepo.at("/name").asText();
                    if (reponame.contains("/")) {
                        fullname = reponame;
                        reponame = reponame.substring(reponame.indexOf("/") + 1);
                    }
                }
                if (rawRepo.has("full_name")) {
                    fullname = rawRepo.at("/full_name").asText();
                }
                if (rawRepo.has("owner")) {
                    ownerId = rawRepo.at("/owner/id").asLong();
                }
                if (rawRepo.at("/html_url").isTextual()) {
                    htmlUrl = rawRepo.at("/html_url").asText();
                    if (htmlUrl.isEmpty() || htmlUrl.equals("null")) {
                        htmlUrl = null;
                    }
                }
                if (rawRepo.at("/description").isTextual()) {
                    description = rawRepo.at("/description").asText();
                    if (description.isEmpty() || description.equals("null")) {
                        description = null;
                    }
                }
                if (rawRepo.has("fork")) {
                    isFork = rawRepo.at("/fork").asBoolean();
                }
                if (rawRepo.at("/homepage").isTextual()) {
                    homepage = rawRepo.at("/homepage").asText();
                    if (homepage.isEmpty() || homepage.equals("null")) {
                        homepage = null;
                    }
                }
                if (rawRepo.has("stargazers_count")) {
                    starCount = rawRepo.at("/stargazers_count").asLong();
                }
                if (rawRepo.has("watchers_count")) {
                    starCount = rawRepo.at("/watchers_count").asLong();
                }
                if (rawRepo.at("/language").isTextual()) {
                    lang = rawRepo.at("/language").asText();
                    if (lang.isEmpty() || lang.equals("null")) {
                        lang = null;
                    }
                }
                if (rawRepo.has("forks_count")) {
                    forkCount = rawRepo.at("/forks_count").asLong();
                }
                if (rawRepo.has("archived")) {
                    isArchive = rawRepo.at("/archived").asBoolean();
                }
                if (rawRepo.has("open_issues_count")) {
                    issueCount = rawRepo.at("/open_issues_count").asLong();
                }
                JsonNode rawLicense = rawRepo.at("/license");
                if (rawLicense.isObject()) {
                    license = rawLicense.at("/name").asText();
                }
                JsonNode rawTopics = rawRepo.at("/topics");
                if (rawTopics.isArray()) {
                    topics = rawTopics.valueStream().map(e -> e.asText()).collect(Collectors.joining(" "));
                }
                if (rawRepo.has("forks")) {
                    forkCount = rawRepo.at("/forks").asLong();
                }
                if (rawRepo.has("open_issues")) {
                    issueCount = rawRepo.at("/open_issues").asLong();
                }
                if (rawRepo.has("watchers")) {
                    starCount = rawRepo.at("/watchers").asLong();
                }
                events.add(new RepoUpdateEvent(
                        id, reponame, fullname, ownerId, htmlUrl, homepage,
                        description, topics, lang, license, isFork, isArchive,
                        forkCount, issueCount, starCount));
            }
        }

        private static void readFromBranchObject(JsonNode rawHead, List<RepoUpdateEvent> events) {
            if (rawHead.isObject()) {
                readFromRepoObject(rawHead.at("/repo"), events);
            }
        }

        private static void readFromIssueObject(JsonNode rawIssue, List<RepoUpdateEvent> events) {
            if (rawIssue.isObject()) {
                readFromBranchObject(rawIssue.at("/head"), events);
                readFromBranchObject(rawIssue.at("/base"), events);
            }
        }

        /**
         * Constructor to create a number of user update events. A single raw event may
         * contain more than a single users information, so this returns a list of
         * updates.
         *
         * @param rawEvent
         *            The raw event JSON object.
         * @return A list of user updates that can be learned from the raw event.
         */
        public static List<RepoUpdateEvent> readFromRawEvent(JsonNode rawEvent) {
            List<RepoUpdateEvent> events = new ArrayList<>();
            readFromRepoObject(rawEvent.at("/repo"), events);
            JsonNode payload = rawEvent.at("/payload");
            if (payload.isObject()) {
                readFromIssueObject(payload.at("/issue"), events);
                readFromIssueObject(payload.at("/pull_request"), events);
                readFromRepoObject(payload.at("/forkee"), events);
            }
            return events;
        }
    }

    /**
     * Create a new table with default values.
     */
    public RepositoriesTable() {
        super();
    }

    @Override
    protected DataStream<RepoUpdateEvent> computeTable() {
        return getRawEventStream()
                .<RepoUpdateEvent>flatMap((rawEvent, out) -> {
                    long seqNum = rawEvent.at("/seq_num").asLong();
                    for (RepoUpdateEvent event : RepoUpdateEvent.readFromRawEvent(rawEvent)) {
                        event.seqNum = seqNum;
                        out.collect(event);
                    }
                })
                .returns(RepoUpdateEvent.class)
                .uid("repo-updates-01")
                .name("Repository Update Stream")
                .keyBy(e -> e.id)
                .filter(new UpdateDeduplicate<>())
                .uid("repo-dedup-updates-01")
                .name("Repository Updates Deduplicated");
    }

    @Override
    protected Class<RepoUpdateEvent> getOutputType() {
        return RepoUpdateEvent.class;
    }
}
