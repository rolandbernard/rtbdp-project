import { useMemo } from "react";
import { useNavigate } from "react-router";

import { useHistoryTime, useLoadingTable } from "../api/hooks";
import type { NormalTable } from "../api/table";
import { groupBy, sort } from "../util";

import { EVENT_KINDS, type EventKind } from "../api/tables";

import StackedAreaBrush from "./charts/StackedAreaBrush";

interface Props<R> {
    table: NormalTable<R>;
    highlight?: EventKind;
}

export default function HistoryMulti<
    R extends { ts_start: string; kind: EventKind; num_events: number }
>(props: Props<R>) {
    const navigate = useNavigate();
    const historyTable = props.table;
    const [loaded, rawHistory] = useLoadingTable(historyTable);
    const lastTime = useHistoryTime(false);
    const [keys, cleanHistory] = useMemo(() => {
        if (!loaded && rawHistory.length < 10) {
            // Avoid initial partial renders.
            return [[], []];
        } else {
            const diff = 300_000;
            const groups = groupBy(rawHistory, "kind").map(rows => {
                const sorted = sort(
                    rows.map(row => ({
                        x: new Date(row.ts_start),
                        y: row.num_events,
                    })),
                    [r => r.x]
                );
                const complete = [];
                let last = {
                    x: new Date(lastTime.getTime() - 60 * 60 * 1000),
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
                while (last.x < lastTime) {
                    last = { x: new Date(last.x.getTime() + diff), y: 0 };
                    complete.push(last);
                }
                return { name: EVENT_KINDS[rows[0]!.kind], data: complete };
            });
            const start = groups
                .map(e => e.data[0]!.x)
                .reduce((a, b) => (a < b ? a : b));
            const end = groups
                .map(e => e.data[e.data.length - 1]!.x)
                .reduce((a, b) => (a > b ? a : b));
            const keys = groups.map(e => e.name);
            const result = [];
            let cur = start;
            while (cur <= end) {
                const row = {
                    x: cur,
                    y: 0,
                    ...Object.fromEntries(
                        groups.map(each => [
                            each.name,
                            each.data[
                                (cur.getTime() - each.data[0]!.x.getTime()) /
                                    diff
                            ]?.y ?? 0,
                        ])
                    ),
                };
                for (const k of keys) {
                    row.y += (row as unknown as { [k: string]: number })[k]!;
                }
                result.push(row);
                cur = new Date(cur.getTime() + diff);
            }
            return [keys, result];
        }
    }, [loaded, rawHistory, lastTime]);
    return (
        <StackedAreaBrush
            keys={keys}
            data={cleanHistory}
            chartColor="var(--color-primary)"
            highligh={
                props.highlight ? EVENT_KINDS[props.highlight] : undefined
            }
            onClick={k =>
                navigate(
                    "/event/" +
                        (props.highlight && k === EVENT_KINDS[props.highlight]
                            ? "all"
                            : Object.entries(EVENT_KINDS).find(
                                  ([_, n]) => n === k
                              )![0]),
                    { replace: true, viewTransition: true }
                )
            }
            window={5 * 60}
        />
    );
}
