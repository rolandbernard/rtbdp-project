import { useMemo, useRef, useSyncExternalStore } from "react";
import { EMPTY } from "rxjs";

import type { Table } from "./table";
import type { Row } from "./client";

export function useLoadingTable<R, V>(
    table: Table<R, V>,
    suppress = false
): [boolean, Row<R>[]] {
    // The view keeps rows from the table between different connections.
    const viewRef = useRef(table.createView());
    const [subscribe, snapshot] = useMemo(() => {
        const view = viewRef.current;
        const events = suppress ? EMPTY : table.connect(view);
        // Initial build of snapshot reusing rows that we already know about.
        let snapshot: [boolean, Row<R>[]] = [
            suppress,
            table.extractFromView(view),
        ];
        return [
            (onChange: () => void) => {
                const subscription = events.subscribe(replayed => {
                    snapshot = [replayed, table.extractFromView(view)];
                    onChange();
                });
                return () => subscription.unsubscribe();
            },
            () => snapshot,
        ];
        // Must be dynamic, since it is based on the filter we apply.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [...table.dependencies(), suppress]);
    return useSyncExternalStore(subscribe, snapshot);
}

export function useTable<R, V>(table: Table<R, V>, suppress = false): Row<R>[] {
    const [_replayed, results] = useLoadingTable(table, suppress);
    return results;
}
