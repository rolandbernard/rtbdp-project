import { createElement, useMemo, useState } from "react";
import { Link } from "react-router";

import { useLoadingTable, useTable } from "../api/hooks";
import {
    countsHistory,
    countsHistoryFine,
    countsLive,
    EVENT_KINDS,
    type EventKind,
    type WindowSize,
} from "../api/tables";
import { useLatched } from "../hooks";
import { sort } from "../util";
import { EVENT_ICONS } from "../utils";

import Counter from "./Counter";
import Selector from "./Selector";
import Sparkline from "./Sparkline";

const SPARK_LINE_SOURCE = {
    "5m": countsHistoryFine.limit(5 * 6),
    "1h": countsHistoryFine.limit(60 * 6),
    "6h": countsHistory.limit(6 * 12),
    "24h": countsHistory.limit(24 * 12),
};

interface Props {
    windowSize: WindowSize;
    kind: EventKind;
}

function EventCounter(props: Props) {
    const [loaded, rawTotal] = useLoadingTable(
        countsLive
            .where("kind", [props.kind])
            .where("window_size", [props.windowSize])
    );
    const total = useLatched(rawTotal[0]?.num_events ?? 0, loaded);
    const historyTable = SPARK_LINE_SOURCE[props.windowSize];
    const isFine = historyTable.name.endsWith("_fine");
    const history = useTable(
        historyTable.where("kind", [props.kind]),
        false,
        props.windowSize
    );
    const data = useMemo(() => {
        const diff = isFine ? 10_000 : 300_000;
        const sorted = sort(
            history.map(row => ({
                x: new Date(row.ts_start),
                y: row.num_events,
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
        return complete;
    }, [history, isFine]);
    return (
        <Link
            to={"/event/" + props.kind}
            className="rounded-box bg-base-300 block border border-border/50"
        >
            <div
                className="flex flex-row items-end rounded-box
                    hover:bg-content/7 hover:dark:bg-content/10 p-1"
            >
                <div className="w-1/2 md:w-1/3 px-1 flex flex-col pe-3 pt-1">
                    <div className="text-xs whitespace-nowrap pb-2">
                        {createElement(EVENT_ICONS[props.kind], {
                            className: "inline w-5 h-5 pe-1",
                        })}
                        {EVENT_KINDS[props.kind]}
                    </div>
                    <Counter
                        value={total}
                        maxDigits={7}
                        className="text-lg pb-2"
                    ></Counter>
                </div>
                <div className="w-1/2 md:w-2/3 h-16">
                    <Sparkline
                        data={data}
                        chartColor="var(--color-primary)"
                        long={!isFine}
                    ></Sparkline>
                </div>
            </div>
        </Link>
    );
}

export default function EventCounts() {
    const [windowSize, setWindowSize] = useState("24h");
    return (
        <div className="flex flex-col flex-auto grow-0">
            <div className="flex flex-row justify-end">
                <div className="w-full max-w-64 pb-1 px-2">
                    <Selector
                        options={{
                            "5m": "5m",
                            "1h": "1h",
                            "6h": "6h",
                            "24h": "24h",
                        }}
                        name="window-size"
                        className="w-full text-sm"
                        value={windowSize}
                        onChange={setWindowSize}
                    ></Selector>
                </div>
            </div>
            <div className="flex flex-wrap">
                {Object.keys(EVENT_KINDS).map(key => (
                    <div
                        key={key}
                        className="p-1 basis-1/2 md:basis-1/3 xl:basis-1/4"
                    >
                        <EventCounter
                            kind={key as EventKind}
                            windowSize={windowSize as WindowSize}
                        ></EventCounter>
                    </div>
                ))}
            </div>
        </div>
    );
}
