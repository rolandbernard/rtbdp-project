import { type Key, type ReactNode } from "react";

import type { Table } from "../api/table";
import type { Row } from "../api/client";
import { useTable } from "../api/hooks";

import SearchBar from "./SearchBar";

interface Props<K, R, V> {
    ident: string;
    find: (ids: K[]) => Table<R, V>;
    search: (query: string) => Table<R, V>;
    id: (row: Row<R>) => K;
    name: (row: Row<R>) => string;
    output: (row: Row<R>, query: string) => ReactNode;
    selected: K[];
    onChange?: (selection: K[]) => void;
    prefix?: ReactNode;
    placeholder?: string;
    object?: string;
    className?: string;
    suppress?: boolean;
    limit?: number;
    debounce?: number;
    rtl?: boolean;
}

export default function SearchSelect<K extends Key, R, V>(
    props: Props<K, R, V>
) {
    const [loaded, selected] =
        useTable(props.find(props.selected)) ?? [];
    return (
        <SearchBar
            name={props.ident}
            table={props.search}
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
                    className="flex flex-row items-center cursor-pointer select-none hover:bg-content/15 px-2 not-xl:px-2 py-px"
                >
                    <input
                        id={props.ident + "-" + props.id(row)}
                        className="w-4 h-4 appearance-none border border-border bg-base-200
                            checked:bg-primary cursor-pointer shrink-0"
                        type="checkbox"
                        checked={props.selected.some(r => r === props.id(row))}
                        onChange={e => {
                            if (props.onChange) {
                                const id = props.id(row);
                                const without = props.selected.filter(
                                    r => r !== id
                                );
                                if (e.target.checked) {
                                    props.onChange([...without, id]);
                                } else {
                                    props.onChange(without);
                                }
                            }
                        }}
                    />
                    <div
                        className={
                            "pl-2 overflow-hidden overflow-ellipsis whitespace-nowrap py-0.5 " +
                            (props.rtl ? " text-left" : "")
                        }
                        style={props.rtl ? { direction: "rtl" } : undefined}
                        title={props.name(row)}
                    >
                        {props.output(row, query)}
                    </div>
                </label>
            )}
            placeholder={
                props.selected.length === 0
                    ? props.placeholder
                    : "Selected " +
                      props.selected.length +
                      " " +
                      (props.object
                          ? props.object +
                            (props.selected.length === 1 ? "." : "s.")
                          : ".")
            }
            placeholderItalic={props.selected.length === 0}
            className={(props.className ?? "") + (loaded ? "" : " loading")}
            suppress={props.suppress}
            limit={props.limit ?? 10}
            debounce={props.debounce}
            default={selected}
        />
    );
}
