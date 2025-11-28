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

public class RepositoriesTable extends AbstractUpdateTable<RepositoriesTable.RepoUpdateEvent> {
    public static class RepoUpdateEvent extends UpdateSeqRow {
        @TableEventKey
        @JsonProperty("id")
        public final long id;
        @JsonProperty("reponame")
        public final String reponame;
        @JsonProperty("fullname")
        public final String fullname;
        @JsonProperty("owner_id")
        public final Long ownerId;
        @JsonProperty("html_url")
        public final String htmlUrl;
        @JsonProperty("homepage")
        public final String homepage;
        @JsonProperty("descr")
        public final String description;
        @JsonProperty("topics")
        public final String topics;
        @JsonProperty("lang")
        public final String lang;
        @JsonProperty("license")
        public final String license;
        @JsonProperty("is_fork")
        public final Boolean isFork;
        @JsonProperty("is_archived")
        public final Boolean isArchive;
        @JsonProperty("fork_count")
        public final Long forkCount;
        @JsonProperty("issue_count")
        public final Long issueCount;
        @JsonProperty("star_count")
        public final Long starCount;

        public RepoUpdateEvent(
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
