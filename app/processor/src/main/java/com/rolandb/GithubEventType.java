package com.rolandb;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * This is a type of event that can be received from the GitHub Events API. Note
 * that these are not the exact types of events as used in the API, but those
 * used for further processing in this application. For example, a number of
 * less important events are combined in the {@code OTHER} category.
 */
public enum GithubEventType {
    // The below should be in sync with the ones in the frontend and in
    // postgres database.
    /** Special type that matches all events. */
    ALL("all"),
    /** A push of one or more commits to a repository. */
    PUSH("push"),
    /** A user starred a repository. */
    WATCH("watch"),
    /** A repository has been created. */
    CREATE_REPO("create_repo"),
    /** A branch has been created. */
    CREATE_BRANCH("create_branch"),
    /** A branch has been deleted. */
    DELETE_BRANCH("delete_branch"),
    /** A repository has been forked. */
    FORK("fork"),
    /** The wiki of a repository has been modified. */
    WIKI("wiki"),
    /** An issue has been opened. */
    ISSUE_OPEN("issue_open"),
    /** An issue has been closed. */
    ISSUE_CLOSE("issue_close"),
    /** An pull request has been opened. */
    PULL_OPEN("pull_open"),
    /** An pull request has been closed. */
    PULL_CLOSE("pull_close"),
    /** An Commit has been commented on. */
    COMMIT_COMMENT("commit_comment"),
    /** An issue has been commented on. */
    ISSUE_COMMENT("issue_comment"),
    /** An pull request has been commented in. */
    PULL_COMMENT("pull_comment"),
    /** All other events get this type. */
    OTHER("other");

    private final String name;

    private GithubEventType(String s) {
        name = s;
    }

    @JsonValue
    @Override
    public String toString() {
        return this.name;
    }

    /**
     * Find the event type matching the given string.
     * 
     * @param name
     *            The name of the event.
     * @return The enum value with that name.
     */
    public static GithubEventType fromString(String name) {
        for (GithubEventType type : GithubEventType.values()) {
            if (type.toString().equals(name)) {
                return type;
            }
        }
        throw new IllegalArgumentException("\"" + name + "\" is not a valid GitHub event type");
    }
}
