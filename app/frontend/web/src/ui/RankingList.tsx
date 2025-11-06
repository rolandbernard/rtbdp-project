import { useEffect, useMemo, useState, type ReactNode } from "react";
import { ArrowLeft, ArrowRight } from "lucide-react";

import type { RankingRow, RankingTable } from "../api/ranking";
import type { Row } from "../api/client";
import { useLoadingTable } from "../api/hooks";
import { sort } from "../util";
import { useParam } from "../hooks";

import Counter, { Letters } from "./Counter";

interface RowProps {
    rank: number;
    children?: ReactNode;
}

const ORDINAL = ["th", "st", "nd", "rd", "th"];

export function RankingRow(props: RowProps) {
    const rank = props.rank + 1;
    return (
        <>
            <div
                className={
                    "flex flex-row justify-end items-center " +
                    (["text-2xl", "text-xl", "text-lg"][props.rank] ?? "")
                }
            >
                <Counter value={rank} />
                <Letters
                    value={ORDINAL[rank % 10] ?? "th"}
                    options={ORDINAL}
                    className="text-xs w-3.5 h-4 mb-2 ml-0.25"
                />
            </div>
            {props.children}
        </>
    );
}

interface ViewProps<R> {
    table: RankingTable<R>;
    from: number;
    to: number;
    rows: (row: Row<RankingRow<R>>) => ReactNode;
    onAtEnd?: (atEnd: boolean) => void;
}

export function RankingView<R>(props: ViewProps<R>) {
    const len = props.to - props.from;
    const [loaded, rawResults] = useLoadingTable(
        props.table.desiredRows(props.from, props.to)
    );
    const results = useMemo(
        () => sort(rawResults, [a => a.row_number]),
        [rawResults]
    );
    const atEnd = rawResults.length < len;
    const { onAtEnd } = props;
    useEffect(() => {
        if (loaded || !atEnd) {
            onAtEnd?.(atEnd);
        }
    }, [loaded, atEnd, onAtEnd]);
    return (
        <>
            {[...results, ...Array(len)].slice(0, len).map((r, i) =>
                r ? (
                    props.rows(r)
                ) : (
                    <div
                        key={i}
                        className="flex-1 flex justify-center items-center text-content/80 col-span-2"
                    >
                        {i === Math.trunc((results.length + len - 1) / 2)
                            ? loaded
                                ? "At the end."
                                : "Loading..."
                            : ""}
                    </div>
                )
            )}
        </>
    );
}

interface Props<R> {
    name: string;
    table: RankingTable<R>;
    rows: (row: Row<RankingRow<R>>) => ReactNode;
}

export default function RankingList<R>(props: Props<R>) {
    const [start, setStart] = useParam(props.name, 0);
    const [pageInput, setPageInput] = useState<string | null>(null);
    const [atEnd, setAtEnd] = useState(false);
    return (
        <div className="flex-1 min-h-0 flex flex-col">
            <div
                className={
                    "flex-1 min-h-0 overflow-hidden grid auto-rows-fr " +
                    (start + 10 < 100
                        ? "grid-cols-[3em_1fr]"
                        : start + 10 < 1000
                        ? "grid-cols-[3.5em_1fr]"
                        : start + 10 < 10000
                        ? "grid-cols-[4em_1fr]"
                        : start + 10 < 100000
                        ? "grid-cols-[4.5em_1fr]"
                        : "grid-cols-[5em_1fr]")
                }
            >
                <RankingView
                    table={props.table}
                    from={start}
                    to={start + 10}
                    rows={props.rows}
                    onAtEnd={e => setAtEnd(e)}
                />
            </div>
            <div className="grow-0 flex flex-row items-center justify-center text-sm">
                <button
                    className="flex items-center justify-center w-8 h-8 mx-1 not-disabled:cursor-pointer
                        rounded-box not-disabled:hover:bg-content/10 not-disabled:dark:hover:bg-content/15 border
                        border-transparent active:border-content/10 disabled:text-transparent"
                    disabled={start === 0}
                    onClick={_e => {
                        const newStart = Math.max(0, start - 10);
                        setStart(newStart);
                        setPageInput(null);
                    }}
                >
                    <ArrowLeft />
                </button>
                <div className="flex flex-row relative items-center overflow-hidden">
                    <input
                        value={
                            pageInput === null
                                ? (start + 1).toString()
                                : pageInput
                        }
                        onChange={e => {
                            const value = parseInt(e.target.value);
                            if (!isNaN(value)) {
                                setStart(Math.max(0, value - 1));
                                setPageInput(null);
                            } else {
                                setPageInput(e.target.value);
                            }
                        }}
                        className="block text-right w-32 pr-16.5 border-2 border-border
                            outline-none text-sm rounded-field focus-visible:border-primary
                            hover:bg-content/3 dark:hover:bg-content/8 pb-0.5"
                    />
                    <div className="absolute h-full w-1/2 top-0 right-0 flex justify-between items-center whitespace-nowrap pb-0.5 select-none">
                        - {start + 10}
                    </div>
                </div>
                <button
                    className="flex items-center justify-center w-8 h-8 mx-1 not-disabled:cursor-pointer
                        rounded-box not-disabled:hover:bg-content/10 not-disabled:dark:hover:bg-content/15 border
                        border-transparent active:border-content/10 disabled:text-transparent"
                    disabled={atEnd}
                    onClick={_e => {
                        const newStart = start + 10;
                        setStart(newStart);
                        setPageInput(null);
                    }}
                >
                    <ArrowRight />
                </button>
            </div>
        </div>
    );
}
