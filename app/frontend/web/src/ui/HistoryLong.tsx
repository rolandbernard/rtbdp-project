import { useMemo } from "react";

import { useHistoryTime, useLoadingTable } from "../api/hooks";
import type { NormalTable } from "../api/table";
import { sort } from "../util";

import AreaBrush from "./charts/AreaBrush";

interface Props<R> {
    table: NormalTable<R>;
    chartColor?: string;
}

export default function HistoryLong<
    R extends { ts_start: string; num_events?: number; num_stars?: number }
>(props: Props<R>) {
    const historyTable = props.table;
    const [loaded, rawHistory] = useLoadingTable(historyTable);
    const lastTime = useHistoryTime(false);
    const cleanHistory = useMemo(() => {
        if (!loaded && rawHistory.length < 10) {
            // Avoid initial partial renders.
            return [];
        } else {
            const diff = 300_000;
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
                        60 * 60 * 1000
                ),
                y: 0,
            };
            for (const row of sorted) {
                while (last.x.getTime() + diff < row.x.getTime()) {
                    last = { x: new Date(last.x.getTime() + diff), y: 0 };
                    complete.push(last);
                }
                complete.push(row);
                last = row;
            }
            if (lastTime) {
                while (last.x < lastTime) {
                    last = { x: new Date(last.x.getTime() + diff), y: 0 };
                    complete.push(last);
                }
            }
            return complete;
        }
    }, [loaded, rawHistory, lastTime]);
    return (
        <AreaBrush
            data={cleanHistory}
            chartColor={props.chartColor ?? "var(--color-primary)"}
            window={5 * 60}
        />
    );
}
