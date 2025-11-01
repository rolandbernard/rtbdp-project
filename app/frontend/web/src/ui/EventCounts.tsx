import { createElement, useMemo, useState } from "react";
import { Link } from "react-router";

import { useLoadingTable } from "../api/hooks";
import {
    countsHistory,
    countsHistoryFine,
    countsLive,
    EVENT_KINDS,
    type EventKind,
    type WindowSize,
} from "../api/tables";
import { useLatched } from "../hooks";
import { EVENT_ICONS } from "../utils";

import Counter from "./Counter";
import Selector from "./Selector";
import HistorySpark from "./HistorySpark";

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
    const history = useMemo(
        () => countsHistory.where("kind", [props.kind]),
        [props.kind]
    );
    const historyFine = useMemo(
        () => countsHistoryFine.where("kind", [props.kind]),
        [props.kind]
    );
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
                    <Counter value={total} className="text-lg pb-2" />
                </div>
                <div className="w-1/2 md:w-2/3 h-16">
                    <HistorySpark
                        table={history}
                        tableFine={historyFine}
                        windowSize={props.windowSize}
                    />
                </div>
            </div>
        </Link>
    );
}

export default function EventCounts() {
    const [windowSize, setWindowSize] = useState("24h");
    return (
        <div className="flex flex-col flex-auto grow-0 mx-1">
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
                    />
                </div>
            </div>
            <div className="flex flex-wrap">
                {Object.keys(EVENT_KINDS).map(key => (
                    <div
                        key={key}
                        className={
                            "p-1 basis-1/2 md:basis-1/3 xl:basis-1/4" +
                            (key === "wiki" ? " md:not-xl:hidden" : "")
                        }
                    >
                        <EventCounter
                            kind={key as EventKind}
                            windowSize={windowSize as WindowSize}
                        />
                    </div>
                ))}
            </div>
        </div>
    );
}
