import {
    BookOpen,
    CheckCircle,
    CircleDot,
    FolderPlus,
    GitBranchMinus,
    GitBranchPlus,
    GitCommit,
    GitFork,
    GitMerge,
    GitPullRequestArrow,
    ListChecks,
    MessageSquare,
    MoreHorizontal,
    Star,
} from "lucide-react";

export const EVENT_ICONS = {
    all: ListChecks,
    push: GitCommit,
    watch: Star,
    create_repo: FolderPlus,
    create_branch: GitBranchPlus,
    delete_branch: GitBranchMinus,
    fork: GitFork,
    wiki: BookOpen,
    issue_open: CircleDot,
    issue_close: CheckCircle,
    pull_open: GitPullRequestArrow,
    pull_close: GitMerge,
    commit_comment: MessageSquare,
    issue_comment: MessageSquare,
    pull_comment: MessageSquare,
    other: MoreHorizontal,
};

export function boldQuery(
    text: string,
    query: string,
    style: string = "font-bold underline"
) {
    const index = text.toLowerCase().indexOf(query.toLowerCase());
    if (index == -1) {
        return <span key="query-highlight">{text}</span>;
    } else {
        return (
            <span key="query-highlight">
                <span>{text.substring(0, index)}</span>
                <span className={style}>
                    {text.substring(index, index + query.length)}
                </span>
                <span>{text.substring(index + query.length)}</span>
            </span>
        );
    }
}
