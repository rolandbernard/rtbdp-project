import { useRef, useSyncExternalStore } from "react";

import { countsHistory, countsHistoryFine } from "./tables";
import type { Table } from "./table";
import type { Row } from "./client";

export function useTable<R, V>(
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
        table.fillFromGlobal(view.current);
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

let latestCoarseHistoryTime: Date | undefined = undefined;
const coarseListeners = new Set<() => void>();
const coarseView = new Map();
const coarseTable = countsHistory.where("kind", ["all"]).limit(1);
coarseTable.connect(coarseView).subscribe(() => {
    const row = coarseTable.extractFromView(coarseView)[0];
    if (row) {
        latestCoarseHistoryTime = new Date(row.ts_start);
        for (const l of coarseListeners) {
            l();
        }
    }
});

function subscribeCoarse(onChange: () => void) {
    // Create a new function to ensure that everything works properly even if
    // a user passes the same `onChange` function multiple times.
    const listener = () => onChange();
    coarseListeners.add(listener);
    return () => coarseListeners.delete(listener);
}

function getCoarse() {
    return latestCoarseHistoryTime;
}

let latestFineHistoryTime: Date | undefined = undefined;
const fineListeners = new Set<() => void>();
const fineView = new Map();
const fineTable = countsHistoryFine.where("kind", ["all"]).limit(1);
fineTable.connect(fineView).subscribe(() => {
    const row = fineTable.extractFromView(fineView)[0];
    if (row) {
        latestFineHistoryTime = new Date(row.ts_start);
        for (const l of fineListeners) {
            l();
        }
    }
});

function subscribeFine(onChange: () => void) {
    // Create a new function to ensure that everything works properly even if
    // a user passes the same `onChange` function multiple times.
    const listener = () => onChange();
    fineListeners.add(listener);
    return () => fineListeners.delete(listener);
}

function getFine() {
    return latestFineHistoryTime;
}

export function useHistoryTime(fine: boolean) {
    return useSyncExternalStore(
        fine ? subscribeFine : subscribeCoarse,
        fine ? getFine : getCoarse
    );
}
