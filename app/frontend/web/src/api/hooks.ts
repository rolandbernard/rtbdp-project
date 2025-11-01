import { useRef, useSyncExternalStore } from "react";

import type { Table } from "./table";
import type { Row } from "./client";

export function useLoadingTable<R, V>(
    table: Table<R, V>,
    suppress = false
): [boolean, Row<R>[]] {
    // The view keeps rows from the table between different connections.
    const view = useRef(table.createView());
    const lastDep = useRef<unknown[]>([]);
    const store = useRef<
        [(onChange: () => void) => () => void, () => [boolean, Row<R>[]]] | null
    >(null);
    const dep = [suppress, ...table.dependencies()];
    if (
        lastDep.current.length != dep.length ||
        lastDep.current.some((e, i) => e != dep[i])
    ) {
        lastDep.current = dep;
        // Initial build of snapshot reusing rows that we already know about.
        let snapshot: [boolean, Row<R>[]] = [
            suppress,
            table.extractFromView(view.current),
        ];
        store.current = [
            (onChange: () => void) => {
                if (suppress) {
                    return () => {};
                } else {
                    const subscription = table
                        .connect(view.current)
                        .subscribe(replayed => {
                            snapshot = [
                                replayed,
                                table.extractFromView(view.current),
                            ];
                            onChange();
                        });
                    return () => subscription.unsubscribe();
                }
            },
            () => snapshot,
        ];
    }
    const [subscribe, snapshot] = store.current!;
    return useSyncExternalStore(subscribe, snapshot);
}

export function useTable<R, V>(table: Table<R, V>, suppress = false): Row<R>[] {
    const [_replayed, results] = useLoadingTable(table, suppress);
    return results;
}
