import { useEffect, useMemo, useState, type ReactNode } from "react";
import { ArrowLeft, ArrowRight } from "lucide-react";

import type { RankingRow, RankingTable } from "../api/ranking";
import type { Row } from "../api/client";
import { useLoadingTable } from "../api/hooks";
import { sort } from "../util";

interface ViewProps<R> {
    table: RankingTable<R>;
    from: number;
    to: number;
    rows: (row: Row<RankingRow<R>>) => ReactNode;
    onAtEnd?: (atEnd: boolean) => void;
}

function RankingView<R>(props: ViewProps<R>) {
    const len = props.to - props.from;
    const [loaded, rawResults] = useLoadingTable(
        props.table.desiredRows(props.from, props.to)
    );
    const results = useMemo(
        () => sort(rawResults, [a => a.row_number]),
        [rawResults]
    );
    const atEnd = loaded && rawResults.length < len;
    const { onAtEnd } = props;
    useEffect(() => {
        onAtEnd?.(atEnd);
    }, [atEnd, onAtEnd]);
    return (
        <>
            {[...results, ...Array(len)]
                .slice(0, len)
                .map((r, i) =>
                    r ? (
                        props.rows(r)
                    ) : (
                        <div className="flex-1 flex justify-center items-center text-content/80">
                            {i === results.length
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
    table: RankingTable<R>;
    rows: (row: Row<RankingRow<R>>) => ReactNode;
}

export default function RankingList<R>(props: Props<R>) {
    const [start, setStart] = useState(0);
    const [pageInput, setPageInput] = useState((start + 1).toString());
    const [atEnd, setAtEnd] = useState(false);
    return (
        <div className="flex-1 min-h-0 flex flex-col">
            <div className="flex-1 min-h-0 overflow-y-scroll overflow-x-hidden flex flex-col">
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
                        setPageInput((newStart + 1).toString());
                    }}
                >
                    <ArrowLeft />
                </button>
                <div className="flex flex-row relative items-center overflow-hidden">
                    <input
                        value={pageInput}
                        onChange={e => {
                            setPageInput(e.target.value);
                            const value = parseInt(e.target.value);
                            if (!isNaN(value)) {
                                setStart(Math.max(0, value - 1));
                            }
                        }}
                        className="block text-right w-32 pr-16.5 border-2 border-border
                            outline-none text-sm rounded-field focus-visible:border-primary
                            hover:bg-content/3 dark:hover:bg-content/8 "
                    />
                    <div className="absolute h-full w-1/2 top-0 right-0 flex justify-between items-center whitespace-nowrap">
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
                        setPageInput((newStart + 1).toString());
                    }}
                >
                    <ArrowRight />
                </button>
            </div>
        </div>
    );
}
