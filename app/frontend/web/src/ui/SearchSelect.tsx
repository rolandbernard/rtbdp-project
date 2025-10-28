import { type ReactNode } from "react";

import type { Table } from "../api/table";
import type { Row } from "../api/client";

import SearchBar from "./SearchBar";

interface Props<R, V> {
    ident: string;
    table: (query: string) => Table<R, V>;
    id: (row: Row<R>) => number | string;
    name: (row: Row<R>) => string;
    output: (row: Row<R>, query: string) => ReactNode;
    selected: Row<R>[];
    prefix?: ReactNode;
    placeholder?: string;
    className?: string;
    suppress?: boolean;
    limit?: number;
}

export default function SearchSelect<R, V>(props: Props<R, V>) {
    return (
        <SearchBar
            name={props.ident}
            table={props.table}
            match={(row, query) => {
                const name = props.name(row);
                return name.toLowerCase().includes(query)
                    ? 1 / (1 + Math.abs(name.length - query.length))
                    : 0;
            }}
            output={(row, query) => (
                <div key={props.id(row)} className="flex flex-row">
                    <input
                        id={props.ident + "-" + props.id(row)}
                        type="checkbox"
                    />
                    <label
                        htmlFor={props.ident + "-" + props.id(row)}
                        className="pl-2 overflow-hidden overflow-ellipsis whitespace-nowrap py-0.5 cursor-pointer"
                    >
                        {props.output(row, query)}
                    </label>
                </div>
            )}
            placeholder={props.placeholder}
            className={props.className}
            suppress={props.suppress}
            limit={props.limit ?? 10}
        />
    );
}
