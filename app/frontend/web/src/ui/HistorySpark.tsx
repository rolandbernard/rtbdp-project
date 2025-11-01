import { useMemo } from "react";
import { useLoadingTable } from "../api/hooks";
import type { NormalTable } from "../api/table";
import type { WindowSize } from "../api/tables";
import Sparkline from "./Sparkline";
import { sort } from "../util";

interface Props<R> {
    table: NormalTable<R>;
    tableFine: NormalTable<R>;
    windowSize: WindowSize;
}

export default function HistorySpark<
    R extends { ts_start: string; num_events?: number; num_stars?: number }
>(props: Props<R>) {
    const historyTable =
        props.windowSize === "5m" || props.windowSize === "1h"
            ? props.tableFine
            : props.table;
    const limit = { "5m": 5 * 6, "1h": 60 * 6, "6h": 6 * 12, "24h": 24 * 6 }[
        props.windowSize
    ];
    const [loaded, rawHistory] = useLoadingTable(historyTable.limit(limit));
    const cleanHistory = useMemo(() => {
        if (!loaded) {
            // Avoid initial partial renders.
            return [];
        } else {
            const diff =
                props.windowSize === "5m" || props.windowSize === "1h"
                    ? 10_000
                    : 300_000;
            const sorted = sort(
                rawHistory.map(row => ({
                    x: new Date(row.ts_start),
                    y: row.num_events ?? row.num_stars ?? 0,
                })),
                [r => r.x]
            );
            const complete = [];
            let last;
            for (const row of sorted) {
                while (last && last.x.getTime() + diff < row.x.getTime()) {
                    last = { x: new Date(last.x.getTime() + diff), y: 0 };
                    complete.push(last);
                }
                complete.push(row);
                last = row;
            }
            complete.splice(0, Math.max(0, complete.length - limit));
            if (props.windowSize === "1h" || props.windowSize === "24h") {
                const factor = props.windowSize === "1h" ? 6 : 4;
                const reduced = [];
                for (let i = complete.length - 1; i >= 0; i -= factor) {
                    let sum = 0;
                    for (let j = 0; j < factor && i - j >= 0; j++) {
                        sum += complete[i - j]!.y;
                    }
                    reduced.push({
                        x: complete[Math.max(0, i + 1 - factor)]!.x,
                        y: sum,
                    });
                }
                return reduced.reverse();
            } else {
                return complete;
            }
        }
    }, [loaded, limit, rawHistory, props.windowSize]);
    return (
        <Sparkline
            data={cleanHistory}
            chartColor="var(--color-primary)"
            window={
                {
                    "5m": 10,
                    "1h": 60,
                    "6h": 5 * 60,
                    "24h": 20 * 60,
                }[props.windowSize]
            }
        ></Sparkline>
    );
}
