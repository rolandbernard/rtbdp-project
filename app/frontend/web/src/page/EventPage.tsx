import { useParams, useViewTransitionState } from "react-router";

import {
    countsHistory,
    countsLive,
    EVENT_KINDS,
    type EventKind,
} from "../api/tables";
import { colorFor } from "../util";

import HistoryLong from "../ui/HistoryLong";
import Proportions from "../ui/Proportions";
import HistoryMulti from "../ui/HistoryMulti";

export default function EventPage() {
    const fromRoot = useViewTransitionState("/");
    const params = useParams();
    const kind = params["kind"]! as EventKind;
    if (!(kind in EVENT_KINDS)) {
        throw new Response("Invalid Kind", {
            status: 404,
            statusText: "Not Found",
        });
    }
    const singleHistoryTable = countsHistory.where("kind", { opt: [kind] });
    const allButAll = Object.keys(EVENT_KINDS).filter(
        e => e !== "all",
    ) as EventKind[];
    const multiHistoryTable = countsHistory.where("kind", { opt: allButAll });
    const proportionsTable = countsLive
        .where("window_size", { opt: ["1h"] })
        .where("kind", { opt: allButAll });
    return (
        <div
            className="flex flex-col grow p-3"
            style={{ viewTransitionName: fromRoot ? "page" : "none" }}
        >
            <div className="text-3xl font-semibold m-3 mt-0 select-text">
                <span style={{ viewTransitionName: "name" }}>
                    {EVENT_KINDS[kind]}
                </span>
                <span
                    style={{ viewTransitionName: fromRoot ? "none" : "events" }}
                >
                    {" "}
                    Events
                </span>
            </div>
            <div className="flex flex-col grow">
                <div
                    className="md:flex-1 not-md:h-128 m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0"
                    style={{ viewTransitionName: "chart" }}
                >
                    <div className="text-xs">Number of Events</div>
                    <HistoryLong
                        table={singleHistoryTable}
                        chartColor={
                            kind !== "all"
                                ? colorFor(EVENT_KINDS[kind])
                                : undefined
                        }
                    />
                </div>
                <div className="flex flex-wrap grow">
                    <div className="md:flex-1 not-md:w-full not-md:h-128 m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0">
                        <div className="text-xs">Events by Type</div>
                        <HistoryMulti
                            table={multiHistoryTable}
                            highlight={kind === "all" ? undefined : kind}
                        />
                    </div>
                    <div className="md:flex-1 not-md:w-full not-md:h-128 m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0">
                        <div className="text-xs">Events in Last Hour</div>
                        <Proportions
                            table={proportionsTable}
                            highlight={kind === "all" ? undefined : kind}
                        />
                    </div>
                </div>
            </div>
        </div>
    );
}
