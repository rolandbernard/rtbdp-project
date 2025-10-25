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
            id = Long.valueOf(rawEvent.at("/id").asText());
            createdAt = event.createdAt;
            eventType = event.eventType;
            repoId = event.repoId;
            userId = event.userId;
            seqNum = event.seqNum;
            StringBuilder builder = new StringBuilder();
            String username = rawEvent.at("/actor/login").asText();
            String reponame = rawEvent.at("/repo/name").asText();
            switch (eventType) {
                case COMMIT_COMMENT: {
                    String commentUrl = rawEvent.at("/payload/comment/html_url").asText();
                    String commitId = rawEvent.at("/payload/comment/commit_id").asText();
                    JsonNode content = rawEvent.at("/payload/comment/body");
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
                    if (content.isTextual()) {
                        builder.append("<quote{");
                        builder.append(content.asText());
                        builder.append("}>");
                    }
                    break;
                }
                case CREATE_BRANCH: {
                    String branchName = rawEvent.at("/payload/ref").asText();
                    JsonNode description = rawEvent.at("/payload/description");
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
                    if (description.isTextual()) {
                        builder.append("<quote{");
                        builder.append(description.asText());
                        builder.append("}>");
                    }
                    break;
                }
                case CREATE_REPO: {
                    JsonNode description = rawEvent.at("/payload/description");
                    builder.append("<user{");
                    builder.append(username);
                    builder.append("}{");
                    builder.append(userId);
                    builder.append("}> created the repository <repo{");
                    builder.append(reponame);
                    builder.append("}{");
                    builder.append(repoId);
                    builder.append("}>.");
                    if (description.isTextual()) {
                        builder.append("<quote{");
                        builder.append(description.asText());
                        builder.append("}>");
                    }
                    break;
                }
                case DELETE_BRANCH: {
                    String tagName = rawEvent.at("/payload/ref").asText();
                    builder.append("<user{");
                    builder.append(username);
                    builder.append("}{");
                    builder.append(userId);
                    builder.append("}> deleted the branch <code{");
                    builder.append(tagName);
                    builder.append("}> in repository <repo{");
                    builder.append(reponame);
                    builder.append("}{");
                    builder.append(repoId);
                    builder.append("}>.");
                    break;
                }
                case FORK: {
                    String forkeeName = rawEvent.at("/payload/forkee/full_name").asText();
                    long forkeeId = rawEvent.at("/payload/forkee/id").asLong();
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
                    String issueUrl = rawEvent.at("/payload/issue/html_url").asText();
                    String title = rawEvent.at("/payload/issue/title").asText();
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
                    String issueUrl = rawEvent.at("/payload/issue/html_url").asText();
                    String commentUrl = rawEvent.at("/payload/comment/html_url").asText();
                    JsonNode content = rawEvent.at("/payload/comment/body");
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
                    if (content.isTextual()) {
                        builder.append("<quote{");
                        builder.append(content.asText());
                        builder.append("}>");
                    }
                    break;
                }
                case ISSUE_OPEN: {
                    String issueUrl = rawEvent.at("/payload/issue/html_url").asText();
                    String title = rawEvent.at("/payload/issue/title").asText();
                    JsonNode content = rawEvent.at("/payload/issue/body");
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
                    if (content.isTextual()) {
                        builder.append("\n\n");
                        builder.append(content.asText());
                    }
                    builder.append("}>");
                    break;
                }
                case PULL_CLOSE: {
                    JsonNode pullRequest = rawEvent.at("/payload/pull_request");
                    String issueUrl = pullRequest.has("html_url")
                            ? pullRequest.at("/html_url").asText()
                            : apiToHtmlUrl(pullRequest.at("/url").asText());
                    String title = pullRequest.at("/title").asText();
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
                    if (rawEvent.at("/payload").has("issue")) {
                        issueUrl = rawEvent.at("/payload/issue/html_url").asText();
                    } else {
                        JsonNode pullRequest = rawEvent.at("/payload/pull_request");
                        issueUrl = pullRequest.has("html_url") ? pullRequest.at("/html_url").asText()
                                : apiToHtmlUrl(pullRequest.at("/url").asText());
                    }
                    String commentUrl = rawEvent.at("/payload/comment/html_url").asText();
                    JsonNode content = rawEvent.at("/payload/comment/body");
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
                    if (content.isTextual()) {
                        builder.append("<quote{");
                        builder.append(content.asText());
                        builder.append("}>");
                    }
                    break;
                }
                case PULL_OPEN: {
                    JsonNode pullRequest = rawEvent.at("/payload/pull_request");
                    String issueUrl = pullRequest.has("html_url")
                            ? pullRequest.at("/html_url").asText()
                            : apiToHtmlUrl(pullRequest.at("/url").asText());
                    String title = pullRequest.at("/title").asText();
                    JsonNode content = pullRequest.at("/body");
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
                    if (content.isTextual()) {
                        builder.append("\n\n");
                        builder.append(content.asText());
                    }
                    builder.append("}>");
                    break;
                }
                case PUSH: {
                    String branchName = rawEvent.at("/payload/ref").asText();
                    long numCommits = rawEvent.at("/payload/size").asLong();
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
                    builder.append("This is an unknown event type <code{");
                    builder.append(rawEvent.at("/type").asText());
                    builder.append("}>.");
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
