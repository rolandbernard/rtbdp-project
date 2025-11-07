import { useParams } from "react-router";

import { countsHistory, EVENT_KINDS, type EventKind } from "../api/tables";

import HistoryLong from "../ui/HistoryLong";

export default function EventPage() {
    const params = useParams();
    const kind = params["kind"]! as EventKind;
    const table = countsHistory.where("kind", [kind]);
    return (
        <div className="flex flex-col min-h-0 grow p-3">
            <div className="text-3xl font-semibold">{EVENT_KINDS[kind]}</div>
            <HistoryLong table={table} />
        </div>
    );
}
