import { useMemo } from "react";

import { useHistoryTime, useTable } from "../api/hooks";
import type { NormalTable } from "../api/table";
import type { WindowSize } from "../api/tables";
import { sort } from "../util";

import Sparkline from "./charts/Sparkline";

interface Props<R> {
    table: NormalTable<R>;
    tableFine: NormalTable<R>;
    windowSize: WindowSize;
    chartColor?: string;
}

export default function HistorySpark<
    R extends { ts_start: string; num_events?: number; num_stars?: number }
>(props: Props<R>) {
    const useFine = props.windowSize === "5m" || props.windowSize === "1h";
    const historyTable = useFine ? props.tableFine : props.table;
    const limit = { "5m": 5 * 6, "1h": 60 * 6, "6h": 6 * 12, "24h": 24 * 12 }[
        props.windowSize
    ];
    const [loaded, rawHistory] = useTable(historyTable.limit(limit));
    const lastTime = useHistoryTime(useFine);
    const cleanHistory = useMemo(() => {
        if (!loaded && rawHistory.length < 10) {
            // Avoid initial partial renders.
            return [];
        } else {
            const diff = useFine ? 10_000 : 300_000;
            const sorted = sort(
                rawHistory.map(row => ({
                    x: new Date(row.ts_start),
                    y: row.num_events ?? row.num_stars ?? 0,
                })),
                [r => r.x]
            );
            const complete = [];
            let last = {
                x: new Date(
                    (
                        lastTime ??
                        sorted[sorted.length - 1]?.x ??
                        new Date()
                    ).getTime() -
                        limit * diff
                ),
                y: 0,
            };
            for (const row of sorted) {
                while (last.x.getTime() + diff < row.x.getTime()) {
                    last = {
                        x: new Date(last.x.getTime() + diff),
                        y: 0,
                    };
                    complete.push(last);
                }
                complete.push(row);
                last = row;
            }
            if (lastTime) {
                while (last.x < lastTime) {
                    last = {
                        x: new Date(last.x.getTime() + diff),
                        y: 0,
                    };
                    complete.push(last);
                }
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
    }, [loaded, limit, rawHistory, useFine, lastTime, props.windowSize]);
    return (
        <Sparkline
            data={cleanHistory}
            chartColor={props.chartColor ?? "var(--color-primary)"}
            window={
                {
                    "5m": 10,
                    "1h": 60,
                    "6h": 5 * 60,
                    "24h": 20 * 60,
                }[props.windowSize]
            }
            className={loaded ? "" : "loading"}
        />
    );
}
