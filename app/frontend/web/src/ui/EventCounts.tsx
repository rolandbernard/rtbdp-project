import { useTable } from "../api/hooks";
import {
    countsLive,
    EVENT_KINDS,
    type EventKind,
    type WindowSize,
} from "../api/tables";
import Counter from "./Counter";

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
            <div>{EVENT_KINDS[props.kind]}</div>
            <Counter value={result[0]?.num_events ?? 0} maxDigits={7}></Counter>
        </div>
    );
}

export default function EventCounts() {
    return (
        <div className="flex flex-wrap items-stretch">
            {Object.keys(EVENT_KINDS).map(key => (
                <div
                    key={key}
                    className="p-1 basis-1/2 md:basis-1/3 xl:basis-1/4 overflow-hidden"
                >
                    <EventCounter
                        kind={key as EventKind}
                        windowSize="24h"
                    ></EventCounter>
                </div>
            ))}
        </div>
    );
}
