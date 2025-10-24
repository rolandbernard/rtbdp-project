import { useMemo, useState } from "react";

import { useLoadingTable, useTable } from "../api/hooks";
import {
    countsHistory,
    countsLive,
    EVENT_KINDS,
    type EventKind,
    type WindowSize,
} from "../api/tables";
import { useLatched } from "../hooks";
import { sort } from "../util";

import Counter from "./Counter";
import Selector from "./Selector";
import Sparkline from "./Sparkline";

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
    const history = useTable(
        countsHistory.where("kind", [props.kind]).limit((7 * 24 * 60) / 5)
    );
    const data = useMemo(() => {
        return sort(
            history.map(row => ({
                x: new Date(row.ts_start),
                y: row.num_events,
            })),
            [r => r.x]
        );
    }, [history]);
    return (
        <div className="rounded-box bg-base-300 p-1 flex flex-row items-center">
            <div className="w-1/2 md:w-1/3 px-2 flex flex-col pe-4">
                <div className="text-xs whitespace-nowrap pb-2">
                    {EVENT_KINDS[props.kind]}
                </div>
                <Counter
                    value={total}
                    maxDigits={7}
                    className="text-lg"
                ></Counter>
            </div>
            <div className="w-1/2 md:w-2/3 h-16">
                <Sparkline
                    data={data}
                    chartColor="var(--color-primary)"
                ></Sparkline>
            </div>
        </div>
    );
}

export default function EventCounts() {
    const [windowSize, setWindowSize] = useState("24h");
    return (
        <div className="flex flex-wrap items-stretch">
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
            <div className="basis-1/2 md:basis-1/3 xl:basis-1/4 flex justify-center items-center">
                <div className="w-2/3">
                    <div className="text-xs pb-0.5">Events in last</div>
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
        </div>
    );
}
