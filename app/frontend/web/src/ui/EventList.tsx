import { createElement, useMemo, useRef, useState } from "react";
import { Link } from "react-router";
import { ArrowUpToLine } from "lucide-react";

import { useLoadingTable } from "../api/hooks";
import {
    EVENT_KINDS,
    events,
    repos,
    users,
    type EventKind,
} from "../api/tables";
import { sort } from "../util";
import { boldQuery, EVENT_ICONS } from "../utils";
import { ConstantTable } from "../api/table";

import SearchSelect from "./SearchSelect";

const DESC_REGEX =
    /<(user\{(.*?)\}\{(.*?)\}|repo\{(.*?)\}\{(.*?)\}|code\{(.*?)\}|quote\{(.*?)\}|link\{(.*?)\}\{(.*?)\})>/gs;

interface DescriptionProps {
    desc: string;
}

function Description(props: DescriptionProps) {
    const parts = [];
    const split = props.desc.split(DESC_REGEX);
    for (let i = 0; i < split.length; i += 10) {
        parts.push(<span key={i}>{split[i]}</span>);
        if (split[i + 2] && split[i + 3]) {
            parts.push(
                <Link
                    key={i + 2}
                    to={"/user/" + split[i + 3]}
                    className="text-primary font-semibold"
                >
                    @{split[i + 2]}
                </Link>
            );
        }
        if (split[i + 4] && split[i + 5]) {
            parts.push(
                <Link
                    key={i + 4}
                    to={"/repo/" + split[i + 5]}
                    className="text-primary font-semibold"
                >
                    {split[i + 4]}
                </Link>
            );
        }
        if (split[i + 6]) {
            parts.push(
                <code
                    key={i + 6}
                    className="font-mono bg-border/25 rounded-selector px-1"
                >
                    {split[i + 6]}
                </code>
            );
        }
        if (split[i + 7]) {
            parts.push(
                <blockquote
                    key={i + 7}
                    className="relative ps-3 text-sm before:bg-border before:absolute
                        before:top-1 before:left-0 before:h-full before:w-1 max-h-32 overflow-hidden"
                >
                    {split[i + 7]}
                </blockquote>
            );
        }
        if (split[i + 8] && split[i + 9]) {
            parts.push(
                <a
                    key={i + 8}
                    target="_blank"
                    href={split[i + 9]}
                    className="text-primary underline"
                >
                    {split[i + 8]}
                </a>
            );
        }
    }
    return <>{parts}</>;
}

interface EventProps {
    created_at: string;
    kind: EventKind;
    desc: string;
}

function Event(props: EventProps) {
    return (
        <div className="bg-base-200 rounded-box my-2 p-2 flex flex-col border border-border/50">
            <div className="flex flex-row justify-between">
                <div className="text-xs flex flex-row items-center">
                    {createElement(EVENT_ICONS[props.kind], {
                        className: "inline w-5 h-5 pe-1",
                    })}
                    {EVENT_KINDS[props.kind]}
                </div>
                <div className="text-xs px-1">
                    {new Date(props.created_at).toLocaleString()}
                </div>
            </div>
            <div className="pt-2 pb-1 px-2 break-words">
                <Description desc={props.desc} />
            </div>
        </div>
    );
}

const eventKinds = new ConstantTable(
    Object.entries(EVENT_KINDS)
        .filter(([key, _name]) => key !== "all")
        .map(([key, name]) => ({
            key,
            name,
        }))
);

export default function EventList() {
    const [showKinds, setKinds] = useState<
        ReturnType<typeof eventKinds.extractFromView>
    >([]);
    const [showUsers, setUsers] = useState<
        ReturnType<typeof users.extractFromView>
    >([]);
    const [showRepos, setRepos] = useState<
        ReturnType<typeof repos.extractFromView>
    >([]);
    const kindIds = useMemo(
        () => showKinds.map(r => r.key as EventKind),
        [showKinds]
    );
    const userIds = useMemo(() => showUsers.map(r => r.id), [showUsers]);
    const repoIds = useMemo(() => showRepos.map(r => r.id), [showRepos]);
    const listRef = useRef<HTMLDivElement>(null);
    let filtered = events.limit(20);
    if (showKinds.length !== 0) {
        filtered = filtered.where("kind", kindIds);
    }
    if (showUsers.length !== 0) {
        filtered = filtered.where("user_id", userIds);
    }
    if (showRepos.length !== 0) {
        filtered = filtered.where("repo_id", repoIds);
    }
    const [loaded, rawResults] = useLoadingTable(filtered);
    const results = useMemo(() => {
        return sort(rawResults, [e => e.created_at, e => e.id], true);
    }, [rawResults]);
    return (
        <div className="flex-1 md:w-full m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0">
            <div className="flex flex-row gap-1 pt-0.5">
                <SearchSelect
                    ident="kind-filter"
                    table={_query => eventKinds}
                    id={row => row.key}
                    name={row => row.name}
                    selected={showKinds}
                    onChange={e => setKinds(e)}
                    output={(row, query) => boldQuery(row.name, query)}
                    className="block w-full"
                    placeholder="Filter by type..."
                    suppress={false}
                    limit={15}
                    debounce={0}
                    object="type"
                />
                <SearchSelect
                    ident="user-filter"
                    table={query =>
                        users.where("username", { substr: query }).limit(10)
                    }
                    id={row => row.id}
                    name={row => row.username!}
                    selected={showUsers}
                    onChange={e => setUsers(e)}
                    output={(row, query) => [
                        <span key="prefix" className="font-bold">
                            @
                        </span>,
                        boldQuery(row.username!, query),
                    ]}
                    className="block w-full"
                    placeholder={"Filter by user..."}
                    object="user"
                />
                <SearchSelect
                    ident="repo-filter"
                    table={query =>
                        repos
                            .where("reponame", { substr: query })
                            .or()
                            .where("fullname", { substr: query })
                            .limit(10)
                    }
                    id={row => row.id}
                    name={row => (row.fullname ?? row.reponame)!}
                    selected={showRepos}
                    onChange={e => setRepos(e)}
                    output={(row, query) =>
                        boldQuery((row.fullname ?? row.reponame)!, query)
                    }
                    className="block w-full"
                    placeholder={"Filter by repository..."}
                    object="repository"
                />
            </div>
            <div className="text-sm flex flex-row justify-center items-start mt-2">
                <button
                    className="text-content/50 hover:text-content/75 cursor-pointer"
                    onClick={() =>
                        listRef.current?.scrollTo({
                            top: 0,
                            behavior: "smooth",
                        })
                    }
                >
                    <ArrowUpToLine className="inline w-4" />
                    <span>newest</span>
                    <ArrowUpToLine className="inline w-4" />
                </button>
            </div>
            <div
                className="grow overflow-y-scroll overflow-x-hidden not-md:h-[50dvh] min-w-0"
                ref={listRef}
            >
                {results.length !== 0 ? (
                    results.map(row => (
                        <Event
                            key={row.id}
                            created_at={row.created_at}
                            kind={row.kind}
                            desc={row.details}
                        />
                    ))
                ) : (
                    <div className="w-full h-full flex justify-center items-center text-content/80">
                        {loaded ? "No such events." : "Loading..."}
                    </div>
                )}
            </div>
        </div>
    );
}
