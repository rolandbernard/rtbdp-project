import { useMemo } from "react";
import { useNavigate } from "react-router";

import { useHistoryTime, useTable } from "../api/hooks";
import type { NormalTable } from "../api/table";
import { groupBy, sortedKey } from "../util";

import { EVENT_KINDS, type EventKind } from "../api/tables";

import StackedAreaBrush, { type MultiDataRow } from "./charts/StackedAreaBrush";

interface Props<R> {
    table: NormalTable<R>;
    highlight?: EventKind;
}

export default function HistoryMulti<
    R extends { ts_start: string; kind: EventKind; num_events: number }
>(props: Props<R>) {
    const navigate = useNavigate();
    const historyTable = props.table;
    const [loaded, rawHistory] = useTable(
        historyTable.limit(12 * 24 * 30 * (Object.keys(EVENT_KINDS).length - 1))
    );
    const lastTime = useHistoryTime(false);
    const [keys, cleanHistory] = useMemo(() => {
        if ((!loaded && rawHistory.length < 10) || rawHistory.length === 0) {
            // Avoid initial partial renders.
            return [[], []];
        } else {
            const diff = 300_000;
            const firstDate = new Date(
                lastTime.getTime() - 12 * 24 * 30 * diff
            ).toISOString();
            const groups = groupBy(
                rawHistory.filter(r => r.ts_start > firstDate),
                "kind"
            ).map(e => e.sort(sortedKey([r => r.ts_start])));
            if (groups.length === 0) {
                return [[], []];
            }
            const starts = groups.map(e => new Date(e[0]!.ts_start));
            const start = starts.reduce((a, b) => (a < b ? a : b));
            const end = groups
                .map(e => new Date(e[e.length - 1]!.ts_start))
                .reduce((a, b) => (a > b ? a : b));
            const keys = groups.map(e => EVENT_KINDS[e[0]!.kind]);
            const result = [];
            let cur = start;
            while (cur <= end) {
                const row: MultiDataRow = {
                    x: cur,
                    y: 0,
                };
                const time = cur.getTime();
                for (const i in groups) {
                    const value =
                        groups[i]![(time - starts[i]!.getTime()) / diff]
                            ?.num_events ?? (loaded ? 0 : NaN);
                    row.y += value;
                    row[keys[i]!] = value;
                }
                result.push(row);
                cur = new Date(time + diff);
            }
            while (cur < lastTime) {
                const time = cur.getTime() + diff;
                result.push({
                    x: new Date(time),
                    y: loaded && time != lastTime.getTime() ? 0 : NaN,
                });
            }
            result.splice(0, Math.max(0, result.length - 12 * 24 * 30));
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
            className={loaded ? "" : "loading"}
        />
    );
}
