import { useState } from "react";
import { useTable } from "../api/hooks";
import {
    countsLive,
    EVENT_KINDS,
    type EventKind,
    type WindowSize,
} from "../api/tables";
import Counter from "./Counter";
import Selector from "./Selector";

interface Props {
    windowSize: WindowSize;
    kind: EventKind;
}

function EventCounter(props: Props) {
    const result = useTable(
        countsLive
            .where("kind", [props.kind])
            .where("window_size", [props.windowSize])
    );
    return (
        <div className="rounded-box bg-base-300 p-2">
            <div className="text-xs">{EVENT_KINDS[props.kind]}</div>
            <Counter
                value={result[0]?.num_events ?? 0}
                maxDigits={7}
            ></Counter>
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
                    className="p-1 basis-1/2 md:basis-1/3 xl:basis-1/4 overflow-hidden"
                >
                    <EventCounter
                        kind={key as EventKind}
                        windowSize={windowSize as WindowSize}
                    ></EventCounter>
                </div>
            ))}
            <div className="basis-1/2 md:basis-1/3 xl:basis-1/4 flex justify-center items-center">
                <div className="w-2/3">
                    <div className="text-xs">Window Size</div>
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
