package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.rolandb.AbstractTable;
import com.rolandb.GithubEvent;
import com.rolandb.GithubEventType;
import com.rolandb.SequencedRow;

import java.time.Instant;

import org.apache.flink.streaming.api.datastream.DataStream;

public class GithubEventsTable extends AbstractTable<GithubEventsTable.DetailedGithubEvent> {
    public static class DetailedGithubEvent extends SequencedRow {
        @TableEventKey
        @JsonProperty("id")
        public final long id;
        @TableEventKey
        @JsonProperty("created_at")
        public final Instant createdAt;
        @JsonProperty("kind")
        public final GithubEventType eventType;
        @JsonProperty("repo_id")
        public final long repoId;
        @JsonProperty("user_id")
        public final long userId;
        @JsonProperty("details")
        public final String details;

        public DetailedGithubEvent(
                long id, Instant createdAt, GithubEventType eventType, long repoId, long userId, String details) {
            this.id = id;
            this.createdAt = createdAt;
            this.eventType = eventType;
            this.repoId = repoId;
            this.userId = userId;
            this.details = details;
        }

        private String apiToHtmlUrl(String url) {
            return url.replace("https://api.github.com/repos", "https://github.com")
                    .replace("/pulls/", "/pull/");
        }

        /**
         * Constructor to create a new detailed event from the raw event data. This
         * constructor is marked with {@code @JsonCreator} so that it will be used
         * automatically for deserialization.
         *
         * @param rawEvent
         *            The raw event JSON object.
         */
        @JsonCreator
        public DetailedGithubEvent(JsonNode rawEvent) {
            GithubEvent event = new GithubEvent(rawEvent);
            id = Long.valueOf(rawEvent.get("id").asText());
            createdAt = event.createdAt;
            eventType = event.eventType;
            repoId = event.repoId;
            userId = event.userId;
            seqNum = event.seqNum;
            StringBuilder builder = new StringBuilder();
            String username = rawEvent.get("actor").get("login").asText();
            String reponame = rawEvent.get("repo").get("name").asText();
            switch (eventType) {
                case COMMIT_COMMENT: {
                    String commentUrl = rawEvent.get("payload").get("comment").get("html_url").asText();
                    String commitId = rawEvent.get("payload").get("comment").get("commit_id").asText();
                    JsonNode content = rawEvent.get("payload").get("comment").get("body");
                    builder.append("<user{");
                    builder.append(username);
                    builder.append("}{");
                    builder.append(userId);
                    builder.append("}> added a <link{comment}{");
                    builder.append(commentUrl);
                    builder.append("}> to commit <link{#");
                    builder.append(commitId.substring(0, 10));
                    builder.append("}{");
                    builder.append(commentUrl.substring(0, commentUrl.indexOf("#")));
                    builder.append("}>.\n");
                    if (content != null && !content.isNull()) {
                        builder.append("<quote{");
                        builder.append(content.asText());
                        builder.append("}>");
                    }
                    break;
                }
                case CREATE_BRANCH: {
                    String branchName = rawEvent.get("payload").get("ref").asText();
                    JsonNode description = rawEvent.get("payload").get("description");
                    builder.append("<user{");
                    builder.append(username);
                    builder.append("}{");
                    builder.append(userId);
                    builder.append("}> created the branch <code{");
                    builder.append(branchName);
                    builder.append("}> in repository <repo{");
                    builder.append(reponame);
                    builder.append("}{");
                    builder.append(repoId);
                    builder.append("}>.");
                    if (description != null && !description.isNull()) {
                        builder.append("<quote{");
                        builder.append(description.asText());
                        builder.append("}>");
                    }
                    break;
                }
                case CREATE_REPO: {
                    JsonNode description = rawEvent.get("payload").get("description");
                    builder.append("<user{");
                    builder.append(username);
                    builder.append("}{");
                    builder.append(userId);
                    builder.append("}> created the repository <repo{");
                    builder.append(reponame);
                    builder.append("}{");
                    builder.append(repoId);
                    builder.append("}>.");
                    if (description != null && !description.isNull()) {
                        builder.append("<quote{");
                        builder.append(description.asText());
                        builder.append("}>");
                    }
                    break;
                }
                case CREATE_TAG: {
                    String tagName = rawEvent.get("payload").get("ref").asText();
                    JsonNode description = rawEvent.get("payload").get("description");
                    builder.append("<user{");
                    builder.append(username);
                    builder.append("}{");
                    builder.append(userId);
                    builder.append("}> created the tag <code{");
                    builder.append(tagName);
                    builder.append("}> in repository <repo{");
                    builder.append(reponame);
                    builder.append("}{");
                    builder.append(repoId);
                    builder.append("}>.");
                    if (description != null && !description.isNull()) {
                        builder.append("<quote{");
                        builder.append(description.asText());
                        builder.append("}>");
                    }
                    break;
                }
                case FORK: {
                    String forkeeName = rawEvent.get("payload").get("forkee").get("full_name").asText();
                    long forkeeId = rawEvent.get("payload").get("forkee").get("id").asLong();
                    builder.append("<user{");
                    builder.append(username);
                    builder.append("}{");
                    builder.append(userId);
                    builder.append("}> created a fork of <repo{");
                    builder.append(reponame);
                    builder.append("}{");
                    builder.append(repoId);
                    builder.append("}> at <repo{");
                    builder.append(forkeeName);
                    builder.append("}{");
                    builder.append(forkeeId);
                    builder.append("}>.");
                    break;
                }
                case ISSUE_CLOSE: {
                    String issueUrl = rawEvent.get("payload").get("issue").get("html_url").asText();
                    String title = rawEvent.get("payload").get("issue").get("title").asText();
                    builder.append("<user{");
                    builder.append(username);
                    builder.append("}{");
                    builder.append(userId);
                    builder.append("}> closed an <link{issue}{");
                    builder.append(issueUrl);
                    builder.append("}> in repository <repo{");
                    builder.append(reponame);
                    builder.append("}{");
                    builder.append(repoId);
                    builder.append("}>.");
                    builder.append("<quote{");
                    builder.append(title);
                    builder.append("}>");
                    break;
                }
                case ISSUE_COMMENT: {
                    String issueUrl = rawEvent.get("payload").get("issue").get("html_url").asText();
                    String commentUrl = rawEvent.get("payload").get("comment").get("html_url").asText();
                    JsonNode content = rawEvent.get("payload").get("comment").get("body");
                    builder.append("<user{");
                    builder.append(username);
                    builder.append("}{");
                    builder.append(userId);
                    builder.append("}> <link{commented}{");
                    builder.append(commentUrl);
                    builder.append("} on an <link{issue}{");
                    builder.append(issueUrl);
                    builder.append("}> in repository <repo{");
                    builder.append(reponame);
                    builder.append("}{");
                    builder.append(repoId);
                    builder.append("}>.");
                    if (content != null && !content.isNull()) {
                        builder.append("<quote{");
                        builder.append(content.asText());
                        builder.append("}>");
                    }
                    break;
                }
                case ISSUE_OPEN: {
                    String issueUrl = rawEvent.get("payload").get("issue").get("html_url").asText();
                    String title = rawEvent.get("payload").get("issue").get("title").asText();
                    JsonNode content = rawEvent.get("payload").get("issue").get("body");
                    builder.append("<user{");
                    builder.append(username);
                    builder.append("}{");
                    builder.append(userId);
                    builder.append("}> opened an <link{issue}{");
                    builder.append(issueUrl);
                    builder.append("}> in repository <repo{");
                    builder.append(reponame);
                    builder.append("}{");
                    builder.append(repoId);
                    builder.append("}>.");
                    builder.append("<quote{");
                    builder.append(title);
                    if (content != null && !content.isNull()) {
                        builder.append("\n\n");
                        builder.append(content.asText());
                    }
                    builder.append("}>");
                    break;
                }
                case PULL_CLOSE: {
                    JsonNode pullRequest = rawEvent.get("payload").get("pull_request");
                    String issueUrl = pullRequest.has("html_url")
                            ? pullRequest.get("html_url").asText()
                            : apiToHtmlUrl(pullRequest.get("url").asText());
                    String title = pullRequest.has("title") ? pullRequest.get("title").asText() : "";
                    builder.append("<user{");
                    builder.append(username);
                    builder.append("}{");
                    builder.append(userId);
                    builder.append("}> closed a <link{pull request}{");
                    builder.append(issueUrl);
                    builder.append("}> in repository <repo{");
                    builder.append(reponame);
                    builder.append("}{");
                    builder.append(repoId);
                    builder.append("}>.");
                    builder.append("<quote{");
                    builder.append(title);
                    builder.append("}>");
                    break;
                }
                case PULL_COMMENT: {
                    String issueUrl;
                    if (rawEvent.get("payload").has("issue")) {
                        issueUrl = rawEvent.get("payload").get("issue").get("html_url").asText();
                    } else {
                        JsonNode pullRequest = rawEvent.get("payload").get("pull_request");
                        issueUrl = pullRequest.has("html_url") ? pullRequest.get("html_url").asText()
                                : apiToHtmlUrl(pullRequest.get("url").asText());
                    }
                    String commentUrl = rawEvent.get("payload").get("comment").get("html_url").asText();
                    JsonNode content = rawEvent.get("payload").get("comment").get("body");
                    builder.append("<user{");
                    builder.append(username);
                    builder.append("}{");
                    builder.append(userId);
                    builder.append("}> <link{commented}{");
                    builder.append(commentUrl);
                    builder.append("}> on a <link{pull request}{");
                    builder.append(issueUrl);
                    builder.append("}> in repository <repo{");
                    builder.append(reponame);
                    builder.append("}{");
                    builder.append(repoId);
                    builder.append("}>.");
                    if (content != null && !content.isNull()) {
                        builder.append("<quote{");
                        builder.append(content.asText());
                        builder.append("}>");
                    }
                    break;
                }
                case PULL_OPEN: {
                    JsonNode pullRequest = rawEvent.get("payload").get("pull_request");
                    String issueUrl = pullRequest.has("html_url")
                            ? pullRequest.get("html_url").asText()
                            : apiToHtmlUrl(pullRequest.get("url").asText());
                    String title = pullRequest.has("title") ? pullRequest.get("title").asText() : "";
                    JsonNode content = pullRequest.get("body");
                    builder.append("<user{");
                    builder.append(username);
                    builder.append("}{");
                    builder.append(userId);
                    builder.append("}> opened a <link{pull request}{");
                    builder.append(issueUrl);
                    builder.append("}> in repository <repo{");
                    builder.append(reponame);
                    builder.append("}{");
                    builder.append(repoId);
                    builder.append("}>.");
                    builder.append("<quote{");
                    builder.append(title);
                    if (content != null && !content.isNull()) {
                        builder.append("\n\n");
                        builder.append(content.asText());
                    }
                    builder.append("}>");
                    break;
                }
                case PUSH: {
                    String branchName = rawEvent.get("payload").get("ref").asText();
                    long numCommits = rawEvent.get("payload").has("size")
                            ? rawEvent.get("payload").get("size").asLong()
                            : 0;
                    builder.append("<user{");
                    builder.append(username);
                    builder.append("}{");
                    builder.append(userId);
                    builder.append("}> pushed ");
                    if (numCommits == 1) {
                        builder.append("a commit ");
                    } else if (numCommits != 0) {
                        builder.append(numCommits);
                        builder.append(" commits ");
                    }
                    builder.append("to repository <repo{");
                    builder.append(reponame);
                    builder.append("}{");
                    builder.append(repoId);
                    builder.append("}> branch <code{");
                    if (branchName.startsWith("refs/heads/")) {
                        builder.append(branchName.substring(11));
                    } else {
                        builder.append(branchName);
                    }
                    builder.append("}>");
                    break;
                }
                case WATCH: {
                    builder.append("<user{");
                    builder.append(username);
                    builder.append("}{");
                    builder.append(userId);
                    builder.append("}> starred the repository <repo{");
                    builder.append(reponame);
                    builder.append("}{");
                    builder.append(repoId);
                    builder.append("}>");
                    break;
                }
                case WIKI: {
                    builder.append("<user{");
                    builder.append(username);
                    builder.append("}{");
                    builder.append(userId);
                    builder.append("}> modified wiki pages for repository <repo{");
                    builder.append(reponame);
                    builder.append("}{");
                    builder.append(repoId);
                    builder.append("}>");
                    break;
                }
                default:
                    builder.append("This is an unknown event type.");
                    break;
            }
            details = builder.toString();
        }
    }

    @Override
    protected DataStream<DetailedGithubEvent> computeTable() {
        return getRawEventStream().map(jsonNode -> new DetailedGithubEvent(jsonNode))
                .name("Detailed Event Stream");
    }

    @Override
    protected Class<DetailedGithubEvent> getOutputType() {
        return DetailedGithubEvent.class;
    }
}
