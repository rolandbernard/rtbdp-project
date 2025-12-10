import { createElement, useMemo, useRef, type RefObject } from "react";
import { Link, useViewTransitionState } from "react-router";
import { ArrowUpToLine } from "lucide-react";

import { useTable } from "../api/hooks";
import {
    EVENT_KINDS,
    events,
    repos,
    users,
    type EventKind,
} from "../api/tables";
import { ConstantTable } from "../api/table";
import { sort } from "../util";
import { boldQuery, EVENT_ICONS } from "../utils";
import { useParam } from "../hooks";

import SearchSelect from "./SearchSelect";

interface LinkProps {
    id: number;
    to: string;
    name: string;
    text?: string;
}

function TransitionLink(props: LinkProps) {
    const inTransition = useViewTransitionState(props.to);
    return (
        <Link
            viewTransition
            to={props.to}
            state={{ from: "events" + props.id, name: props.name }}
            className="text-primary font-semibold dark:hover:text-primary/90 hover:text-primary/75"
            style={{
                viewTransitionName: inTransition
                    ? "pageevents" + props.id
                    : "none",
            }}
        >
            <span
                style={{
                    viewTransitionName: inTransition
                        ? "nameevents" + props.id
                        : "none",
                }}
            >
                {props.text ?? props.name}
            </span>
        </Link>
    );
}

const DESC_REGEX =
    /<(user\{(.*?)\}\{(.*?)\}|repo\{(.*?)\}\{(.*?)\}|code\{(.*?)\}|quote\{(.*?)\}|link\{(.*?)\}\{(.*?)\})>/gs;

interface DescriptionProps {
    id: number;
    desc: string;
}

function Description(props: DescriptionProps) {
    const parts = [];
    const split = props.desc.split(DESC_REGEX);
    for (let i = 0; i < split.length; i += 10) {
        parts.push(<span key={i}>{split[i]}</span>);
        if (split[i + 2] && split[i + 3]) {
            parts.push(
                <TransitionLink
                    key={i + 2}
                    id={props.id}
                    to={"/user/" + split[i + 3]}
                    text={"@" + split[i + 2]}
                    name={split[i + 2]!}
                />
            );
        }
        if (split[i + 4] && split[i + 5]) {
            parts.push(
                <TransitionLink
                    key={i + 4}
                    id={props.id}
                    to={"/repo/" + split[i + 5]}
                    name={split[i + 4]!}
                />
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
                    {split[i + 7]!.length > 256
                        ? split[i + 7]!.substring(0, 256)
                        : split[i + 7]}
                </blockquote>
            );
        }
        if (split[i + 8] && split[i + 9]) {
            parts.push(
                <a
                    key={i + 8}
                    target="_blank"
                    href={split[i + 9]}
                    className="text-primary underline dark:hover:text-primary/90 hover:text-primary/75"
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
    id: number;
}

function Event(props: EventProps) {
    return (
        <div className="bg-base-200 rounded-box my-2 p-2 flex flex-col border border-border/50 contain-content">
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
            <div className="pt-2 pb-1 px-2 wrap-break-word select-text">
                <Description desc={props.desc} id={props.id} />
            </div>
        </div>
    );
}

interface Props {
    kindIds: EventKind[];
    userIds: number[];
    repoIds: number[];
    listRef?: RefObject<HTMLDivElement | null>;
}

function BasicEventList(props: Props) {
    let filtered = events.limit(20);
    if (props.kindIds.length !== 0) {
        filtered = filtered.where("kind", props.kindIds);
    }
    if (props.userIds.length !== 0) {
        filtered = filtered.where("user_id", props.userIds);
    }
    if (props.repoIds.length !== 0) {
        filtered = filtered.where("repo_id", props.repoIds);
    }
    const [loaded, rawResults] = useTable(filtered);
    const results = useMemo(() => {
        return sort(rawResults, [e => e.created_at, e => e.id], true);
    }, [rawResults]);
    return (
        <div
            className={
                "grow not-md:h-140 min-h-0 min-w-0 " + (loaded ? "" : "loading")
            }
        >
            {results.length !== 0 ? (
                <div
                    className="h-full w-full min-h-0 overflow-y-scroll overflow-x-hidden"
                    ref={props.listRef}
                >
                    {results.map(row => (
                        <Event
                            key={row.id}
                            created_at={row.created_at}
                            kind={row.kind}
                            desc={row.details}
                            id={row.id}
                        />
                    ))}
                </div>
            ) : (
                <div className="w-full h-full flex justify-center items-center text-content/80">
                    {loaded ? "No such events." : "Loading..."}
                </div>
            )}
        </div>
    );
}

const eventKinds = new ConstantTable(
    Object.entries(EVENT_KINDS)
        .filter(([key, _name]) => key !== "all")
        .map(([key, name]) => ({
            key: key as EventKind,
            name,
        }))
);

const EMPTY: never[] = [];

export default function EventList() {
    const [kindIds, setKindIds] = useParam("elkind", EMPTY as EventKind[]);
    const [userIds, setUserIds] = useParam("eluser", EMPTY as number[]);
    const [repoIds, setRepoIds] = useParam("elrepo", EMPTY as number[]);
    const listRef = useRef<HTMLDivElement>(null);
    return (
        <div className="flex-1 md:w-full m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0">
            <div className="flex flex-row gap-1 pt-0.5">
                <SearchSelect
                    ident="kind-filter"
                    find={_ids => eventKinds}
                    search={_query => eventKinds}
                    id={row => row.key}
                    name={row => row.name}
                    selected={kindIds}
                    onChange={e => setKindIds(e)}
                    output={(row, query) =>
                        boldQuery(row.name, query, "font-semibold underline")
                    }
                    className="block w-full"
                    placeholder="Filter by type..."
                    suppress={false}
                    limit={15}
                    debounce={0}
                    object="type"
                />
                <SearchSelect
                    ident="user-filter"
                    find={ids => users.where("id", ids)}
                    search={query =>
                        users
                            .where("username", [query])
                            .or()
                            .where("username", { substr: query })
                            .limit(10)
                    }
                    id={row => row.id}
                    name={row => row.username!}
                    selected={userIds}
                    onChange={e => setUserIds(e)}
                    output={(row, query) => [
                        <span key="prefix" className="font-semibold">
                            @
                        </span>,
                        boldQuery(
                            row.username!,
                            query,
                            "font-semibold underline"
                        ),
                    ]}
                    className="block w-full"
                    placeholder={"Filter by user..."}
                    object="user"
                />
                <SearchSelect
                    ident="repo-filter"
                    find={ids => repos.where("id", ids)}
                    search={query =>
                        repos
                            .where("reponame", [query])
                            .or()
                            .where("fullname", [query])
                            .or()
                            .where("reponame", { substr: query })
                            .or()
                            .where("fullname", { substr: query })
                            .limit(10)
                    }
                    id={row => row.id}
                    name={row => (row.fullname ?? row.reponame)!}
                    selected={repoIds}
                    onChange={e => setRepoIds(e)}
                    output={(row, query) =>
                        boldQuery(
                            (row.fullname ?? row.reponame)!,
                            query,
                            "font-semibold underline"
                        )
                    }
                    className="block w-full"
                    placeholder={"Filter by repository..."}
                    object="repository"
                    rtl={true}
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
            <BasicEventList
                kindIds={kindIds}
                repoIds={repoIds}
                userIds={userIds}
                listRef={listRef}
            />
        </div>
    );
}
