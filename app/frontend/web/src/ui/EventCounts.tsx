import { createElement, useMemo } from "react";
import { Link, useViewTransitionState } from "react-router";

import { useTable } from "../api/hooks";
import {
    countsHistory,
    countsHistoryFine,
    countsLive,
    EVENT_KINDS,
    type EventKind,
    type WindowSize,
} from "../api/tables";
import { useLatched, useParam } from "../hooks";
import { EVENT_ICONS } from "../utils";
import { colorFor } from "../util";

import Counter from "./Counter";
import Selector from "./Selector";
import HistorySpark from "./HistorySpark";

interface Props {
    windowSize: WindowSize;
    kind: EventKind;
}

function EventCounterOnly(props: Props) {
    const [loaded, rawTotal] = useTable(
        countsLive
            .where("kind", [props.kind])
            .where("window_size", [props.windowSize])
    );
    const total = useLatched(rawTotal[0]?.num_events ?? 0, loaded);
    return (
        <Counter
            value={total}
            className={"text-lg pb-2 " + (loaded ? "" : "loading")}
            maxDigits={7}
        />
    );
}

function EventCounter(props: Props) {
    const inTransition = useViewTransitionState("/event/" + props.kind);
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
            viewTransition
            to={"/event/" + props.kind}
            className="rounded-box bg-base-200 block border border-border/50"
            style={{
                viewTransitionName: inTransition ? "page" : "none",
            }}
        >
            <div
                className="flex flex-row items-end rounded-box
                    hover:bg-content/5 hover:dark:bg-content/7 p-1"
            >
                <div className="w-1/2 md:w-1/3 px-1 flex flex-col pe-3 pt-1">
                    <div className="text-xs whitespace-nowrap pb-2">
                        {createElement(EVENT_ICONS[props.kind], {
                            className: "inline w-5 h-5 pe-1",
                        })}
                        <span
                            style={{
                                viewTransitionName: inTransition
                                    ? "name"
                                    : "none",
                            }}
                        >
                            {EVENT_KINDS[props.kind]}
                        </span>
                    </div>
                    <EventCounterOnly
                        kind={props.kind}
                        windowSize={props.windowSize}
                    />
                </div>
                <div
                    className="w-1/2 md:w-2/3 h-16"
                    style={{
                        viewTransitionName: inTransition ? "chart" : "none",
                    }}
                >
                    <HistorySpark
                        table={history}
                        tableFine={historyFine}
                        windowSize={props.windowSize}
                        chartColor={
                            props.kind !== "all"
                                ? colorFor(EVENT_KINDS[props.kind])
                                : undefined
                        }
                    />
                </div>
            </div>
        </Link>
    );
}

export default function EventCounts() {
    const [windowSize, setWindowSize] = useParam<WindowSize>("ecwin", "24h");
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
                        onChange={w => setWindowSize(w)}
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
