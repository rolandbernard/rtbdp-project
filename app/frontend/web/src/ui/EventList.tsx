import { useMemo, useRef } from "react";
import { Link } from "react-router";

import { useLoadingTable } from "../api/hooks";
import { EVENT_KINDS, events } from "../api/tables";
import { sort } from "../util";
import { ArrowUpToLine } from "lucide-react";

const DESC_REGEX =
    /<(user\{(.*?)\}\{(.*?)\}|repo\{(.*?)\}\{(.*?)\}|code\{(.*?)\}|quote\{(.*?)\}|link\{(.*?)\}\{(.*?)\})>/gs;

function transformDescription(desc: string) {
    const parts = [];
    const split = desc.split(DESC_REGEX);
    for (let i = 0; i < split.length; i += 10) {
        parts.push(split[i]);
        if (split[i + 2] && split[i + 3]) {
            parts.push(
                <Link
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
                    to={"/repo/" + split[i + 5]}
                    className="text-primary font-semibold"
                >
                    {split[i + 4]}
                </Link>
            );
        }
        if (split[i + 6]) {
            parts.push(
                <code className="font-mono bg-border/25 rounded-selector px-1">
                    {split[i + 6]}
                </code>
            );
        }
        if (split[i + 7]) {
            parts.push(
                <blockquote
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
                    target="_blank"
                    href={split[i + 9]}
                    className="text-primary underline"
                >
                    {split[i + 8]}
                </a>
            );
        }
    }
    return parts;
}

export default function EventList() {
    const listRef = useRef<HTMLDivElement>(null);
    const [loaded, rawResults] = useLoadingTable(events.limit(100));
    const results = useMemo(() => {
        return sort(rawResults, [e => e.created_at, e => e.id], true);
    }, [rawResults]);
    return (
        <div className="md:w-full m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0">
            <div className="flex flex-row gap-1 pt-0.5">
                <input
                    type="text"
                    className="block w-full px-3 border-2 border-border outline-none text-sm
                        rounded-field py-2 focus-visible:border-primary placeholder:text-content/65
                        placeholder:italic hover:bg-content/3 dark:hover:bg-content/8"
                    placeholder="Filter by kind..."
                ></input>
                <input
                    type="text"
                    className="block w-full px-3 border-2 border-border outline-none text-sm
                        rounded-field py-2 focus-visible:border-primary placeholder:text-content/65
                        placeholder:italic hover:bg-content/3 dark:hover:bg-content/8"
                    placeholder="Filter by user..."
                ></input>
                <input
                    type="text"
                    className="block w-full px-3 border-2 border-border outline-none text-sm
                        rounded-field py-2 focus-visible:border-primary placeholder:text-content/65
                        placeholder:italic hover:bg-content/3 dark:hover:bg-content/8"
                    placeholder="Filter by repository..."
                ></input>
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
                className="grow overflow-y-scroll overflow-x-hidden max-h-[75dvh] min-w-0"
                ref={listRef}
            >
                {results.length !== 0 ? (
                    results.map(row => (
                        <div
                            key={row.id}
                            className="bg-base-200 rounded-box my-2 p-2 flex flex-col border border-border/50"
                        >
                            <div className="flex flex-row justify-between">
                                <div className="text-xs">
                                    {EVENT_KINDS[row.kind]}
                                </div>
                                <div className="text-xs px-1">
                                    {new Date(row.created_at).toLocaleString()}
                                </div>
                            </div>
                            <div className="pt-2 pb-1 px-2">
                                {transformDescription(row.details)}
                            </div>
                        </div>
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
