import { useMemo } from "react";

import { useHistoryTime, useLoadingTable } from "../api/hooks";
import type { NormalTable } from "../api/table";

import PieChart from "./charts/PieChart";
import { EVENT_KINDS, type EventKind } from "../api/tables";

interface Props<R> {
    table: NormalTable<R>;
    highlight?: EventKind;
}

export default function Proportions<
    R extends { kind: EventKind; num_events: number }
>(props: Props<R>) {
    const table = props.table;
    const [loaded, rawData] = useLoadingTable(table);
    const lastTime = useHistoryTime(false);
    const cleanData = useMemo(() => {
        if (!loaded || rawData.length === 0) {
            // Avoid initial partial renders.
            return [];
        } else {
            return rawData.map(row => ({
                x: EVENT_KINDS[row.kind],
                y: row.num_events,
            }));
        }
    }, [loaded, rawData, lastTime]);
    return (
        <PieChart
            data={cleanData}
            highligh={
                props.highlight ? EVENT_KINDS[props.highlight] : undefined
            }
        />
    );
}
