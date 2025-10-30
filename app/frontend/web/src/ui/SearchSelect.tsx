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
    onChange?: (selection: Row<R>[]) => void;
    prefix?: ReactNode;
    placeholder?: string;
    className?: string;
    suppress?: boolean;
    limit?: number;
    debounce?: number;
}

export default function SearchSelect<R, V>(props: Props<R, V>) {
    return (
        <SearchBar
            name={props.ident}
            table={props.table}
            match={(row, query) => {
                if (query.length === 0) {
                    return 1;
                } else {
                    const name = props.name(row);
                    return name.toLowerCase().includes(query)
                        ? 1 / (1 + Math.abs(name.length - query.length))
                        : 0;
                }
            }}
            output={(row, query) => (
                <label
                    key={props.id(row)}
                    htmlFor={props.ident + "-" + props.id(row)}
                    className="flex flex-row items-center cursor-pointer select-none"
                >
                    <input
                        id={props.ident + "-" + props.id(row)}
                        className="w-4 h-4 appearance-none border-1 border-border bg-base-200
                            checked:bg-green-700 cursor-pointer"
                        type="checkbox"
                        defaultChecked={props.selected.some(
                            r => props.id(r) === props.id(row)
                        )}
                        onChange={e => {
                            if (props.onChange) {
                                const without = props.selected.filter(
                                    r => props.id(r) !== props.id(row)
                                );
                                if (e.target.checked) {
                                    props.onChange([...without, row]);
                                } else {
                                    props.onChange(without);
                                }
                            }
                        }}
                    />
                    <div className="pl-2 overflow-hidden overflow-ellipsis whitespace-nowrap py-0.5">
                        {props.output(row, query)}
                    </div>
                </label>
            )}
            placeholder={
                props.selected.length === 0
                    ? props.placeholder
                    : "Selected " + props.selected.length
            }
            placeholderItalic={props.selected.length === 0}
            className={props.className}
            suppress={props.suppress}
            limit={props.limit ?? 10}
            debounce={props.debounce}
            default={props.selected}
        />
    );
}
