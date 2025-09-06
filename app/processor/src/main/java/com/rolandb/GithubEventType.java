package com.rolandb;

/**
 * This is a type of event that can be received from the GitHub Events API. Note
 * that these are not the exact types of events as used in the API, but those
 * used for further processing in this application. For example, a number of
 * less important events are combined in the {@code OTHER} category.
 */
public enum GithubEventType {
    ALL("all"),
    PUSH("push"),
    WATCH("watch"),
    CREATE_REPO("create_repo"),
    CREATE_BRANCH("create_branch"),
    CREATE_TAG("create_tag"),
    FORK("fork"),
    WIKI("wiki"),
    ISSUE_OPEN("issue_open"),
    ISSUE_CLOSE("issue_close"),
    PULL_OPEN("pull_open"),
    PULL_CLOSE("pull_close"),
    COMMIT_COMMENT("commit_comment"),
    ISSUE_COMMENT("issue_comment"),
    PULL_COMMENT("pull_comment"),
    OTHER("other");

    private final String name;

    private GithubEventType(String s) {
        name = s;
    }

    @Override
    public String toString() {
        return this.name;
    }

    public static GithubEventType fromString(String name) {
        for (GithubEventType type : GithubEventType.values()) {
            if (type.toString().equals(name)) {
                return type;
            }
        }
        throw new IllegalArgumentException("\"" + name + "\" is not a valid GitHub event type");
    }
}
