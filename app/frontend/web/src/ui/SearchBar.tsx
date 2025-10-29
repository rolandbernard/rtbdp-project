import { useState, type ReactNode } from "react";

import { useLoadingTable } from "../api/hooks";
import { useClickOutside, useDebounce } from "../hooks";
import { sort } from "../util";
import type { Table } from "../api/table";
import type { Row } from "../api/client";

interface Props<R, V> {
    name: string;
    table: (query: string) => Table<R, V>;
    match: (row: Row<R>, query: string) => number;
    output: (row: Row<R>, query: string) => ReactNode;
    prefix?: ReactNode;
    limit?: number;
    default?: Row<R>[];
    debounce?: number;
    autoFocus?: boolean;
    placeholder?: string;
    placeholderItalic?: boolean;
    className?: string;
    suppress?: boolean;
}

export default function SearchBar<R, V>(props: Props<R, V>) {
    const [query, setQuery] = useState("");
    const lowerQuery = query.toLowerCase();
    const debounced = useDebounce(lowerQuery, props.debounce ?? 250);
    const [complete, results] = useLoadingTable(
        props.table(debounced),
        props.suppress !== false && debounced.length === 0
    );
    const filteredResults = (
        props.suppress !== false && query.length === 0
            ? props.default ?? []
            : results
    ).filter(row => props.match(row, lowerQuery) > 0);
    const trueResults = sort(
        filteredResults,
        [row => props.match(row, lowerQuery)],
        true
    ).slice(0, props.limit ?? filteredResults.length);
    useClickOutside("div#search-bar-" + props.name, () => setQuery(""));
    return (
        <div
            id={"search-bar-" + props.name}
            className={"relative " + (props.className ?? "")}
        >
            <div className="peer w-full relative block">
                {props.prefix}
                <input
                    type="text"
                    className={
                        "block w-full px-3 py-2 border-2 border-border outline-none text-sm " +
                        "rounded-field focus-visible:border-primary " +
                        "hover:bg-content/3 dark:hover:bg-content/8 " +
                        (props.placeholderItalic !== false
                            ? "placeholder:text-content/65 placeholder:italic "
                            : "placeholder:text-content/80 ")
                    }
                    placeholder={props.placeholder}
                    value={query}
                    autoFocus={props.autoFocus}
                    onChange={e => setQuery(e.target.value)}
                />
            </div>
            {props.suppress === false ||
            (props.default && props.default.length !== 0) ||
            query.length !== 0 ? (
                <div
                    className="absolute inset-y-full end-0 w-full hidden focus-within:block
                        peer-focus-within:block hover:block active:block z-50"
                >
                    <div className="flex flex-col w-full bg-base-300 rounded-box p-4 py-3 shadow-xl dark:shadow-2xl">
                        {trueResults.length ? (
                            trueResults.map(row =>
                                props.output(row, lowerQuery)
                            )
                        ) : (
                            <div className="text-content/80">
                                {query !== debounced || !complete
                                    ? "Searching..."
                                    : "No results found."}
                            </div>
                        )}
                    </div>
                </div>
            ) : undefined}
        </div>
    );
}
