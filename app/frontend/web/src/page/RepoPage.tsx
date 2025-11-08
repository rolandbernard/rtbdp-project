import { Link, useParams } from "react-router";

import { useLoadingTable, useTable } from "../api/hooks";
import { repos, reposHistory, starsHistory, users } from "../api/tables";
import HistoryLong from "../ui/HistoryLong";

interface OwnerProps {
    id?: number;
    name?: string;
}

function OwnerCard(props: OwnerProps) {
    const table = props.id
        ? users.where("id", [props.id])
        : users.where("username", [props.name]);
    const user = useTable(table)[0];
    return user ? (
        <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0">
            <div className="text-xs pb-1">Owner</div>
            <Link
                to={"/user/" + user.id}
                className={
                    "text-primary font-semibold " +
                    (user?.username ? "" : "text-primary/50")
                }
                title={user?.username}
            >
                <span className="font-bold">@</span>
                {user?.username}
            </Link>
        </div>
    ) : props.name ? (
        <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0">
            <div className="text-xs pb-1">Owner</div>
            <div className="font-semibold">
                <span className="font-bold">@</span>
                {props.name}
            </div>
        </div>
    ) : undefined;
}

interface DetailsProps {
    reponame?: string;
    fullname?: string;
    owner_id?: number;
    html_url?: string;
    homepage?: string;
    descr?: string;
    topics?: string;
    lang?: string;
    license?: string;
    fork_count?: number;
    issue_count?: number;
    star_count?: number;
}

function RepoDetails(props: DetailsProps) {
    return (
        <div className="flex flex-wrap gap-3">
            {props.reponame || props.fullname ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0">
                    <div className="text-xs pb-1">Repository Name</div>
                    {props.reponame ??
                        props.fullname?.substring(
                            props.fullname.indexOf("/") + 1
                        )}
                </div>
            ) : undefined}
            {props.owner_id || props.fullname ? (
                <OwnerCard
                    id={props.owner_id}
                    name={
                        props.fullname
                            ? props.fullname.substring(
                                  0,
                                  props.fullname.indexOf("/")
                              )
                            : undefined
                    }
                />
            ) : undefined}
            {props.html_url || props.fullname ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0">
                    <div className="text-xs pb-1">GitHub URL</div>
                    <a
                        href={
                            props.html_url ??
                            "https://github.com/" + props.fullname
                        }
                        target="_blank"
                        className="text-primary underline"
                    >
                        {props.html_url ??
                            "https://github.com/" + props.fullname}
                    </a>
                </div>
            ) : undefined}
            {props.homepage ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0">
                    <div className="text-xs pb-1">Homepage</div>
                    <a
                        href={props.homepage}
                        target="_blank"
                        className="text-primary underline"
                    >
                        {props.homepage}
                    </a>
                </div>
            ) : undefined}
            {props.topics ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0">
                    <div className="text-xs pb-1">Topics</div>
                    {props.topics.split(" ").map(e => (
                        <span className="pe-1">{e}</span>
                    ))}
                </div>
            ) : undefined}
            {props.lang ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0">
                    <div className="text-xs pb-1">Language</div>
                    {props.lang}
                </div>
            ) : undefined}
            {props.license ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0">
                    <div className="text-xs pb-1">License</div>
                    {props.license}
                </div>
            ) : undefined}
            {props.fork_count ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0">
                    <div className="text-xs pb-1">Forks</div>
                    {props.fork_count}
                </div>
            ) : undefined}
            {props.issue_count ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0">
                    <div className="text-xs pb-1">Issues</div>
                    {props.issue_count}
                </div>
            ) : undefined}
            {props.star_count ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0">
                    <div className="text-xs pb-1">Stars</div>
                    {props.star_count}
                </div>
            ) : undefined}
            {props.descr ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0">
                    <div className="text-xs pb-1">Description</div>
                    {props.descr}
                </div>
            ) : undefined}
        </div>
    );
}

export default function RepoPage() {
    const params = useParams();
    const repoId = parseInt(params["repoId"]!);
    const [loaded, repoData] = useLoadingTable(repos.where("id", [repoId]));
    const repo = repoData[0];
    if (loaded && !repo) {
        throw new Response("Invalid Repository", {
            status: 404,
            statusText: "Not Found",
        });
    }
    return (
        <div className="flex flex-col grow p-3">
            <div className="text-3xl font-semibold m-3 mt-0">
                {repo ? (
                    repo?.fullname ?? repo?.reponame
                ) : (
                    <span className="text-content/80">Loading...</span>
                )}
            </div>
            <div className="flex flex-col grow">
                <div className="m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0 min-h-20">
                    {repo ? (
                        <RepoDetails {...repo} />
                    ) : (
                        <div className="w-full h-full flex justify-center items-center text-content/80">
                            Loading...
                        </div>
                    )}
                </div>
                <div className="md:flex-1 not-md:h-[50dvh] m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0">
                    <div className="text-xs">Rankings</div>
                    {repo ? (
                        <></>
                    ) : (
                        <div className="w-full h-full flex justify-center items-center text-content/80">
                            Loading...
                        </div>
                    )}
                </div>
                <div className="flex flex-wrap grow">
                    <div className="md:flex-1 not-md:w-full not-md:h-[50dvh] m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0">
                        <div className="text-xs">Activity History</div>
                        <HistoryLong
                            table={reposHistory.where("repo_id", [repoId])}
                        />
                    </div>
                    <div className="md:flex-1 not-md:w-full not-md:h-[50dvh] m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0">
                        <div className="text-xs">Stars History</div>
                        <HistoryLong
                            table={starsHistory.where("repo_id", [repoId])}
                        />
                    </div>
                </div>
            </div>
        </div>
    );
}
