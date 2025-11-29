import { useMemo } from "react";
import { useNavigate } from "react-router";

import { useTable } from "../api/hooks";
import { EVENT_KINDS, type EventKind } from "../api/tables";
import type { NormalTable } from "../api/table";

import PieChart from "./charts/PieChart";

interface Props<R> {
    table: NormalTable<R>;
    highlight?: EventKind;
}

export default function Proportions<
    R extends { kind: EventKind; num_events: number }
>(props: Props<R>) {
    const navigate = useNavigate();
    const table = props.table;
    const [loaded, rawData] = useTable(table);
    const cleanData = useMemo(() => {
        if (!loaded && rawData.length < 10) {
            // Avoid initial partial renders.
            return [];
        } else {
            return rawData.map(row => ({
                x: EVENT_KINDS[row.kind],
                y: row.num_events,
            }));
        }
    }, [loaded, rawData]);
    return (
        <PieChart
            data={cleanData}
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
            className={loaded ? "" : "loading"}
        />
    );
}
