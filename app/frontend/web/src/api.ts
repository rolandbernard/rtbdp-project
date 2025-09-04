import { webSocket } from "rxjs/webSocket";
import { filter, map, retry, throttleTime } from "rxjs/operators";
import { useMemo, useRef, useSyncExternalStore } from "react";

type Filter<T> = T[] | { start?: T; end?: T };
type RowFilter<R> = { [P in keyof R]?: Filter<R[P]> };
type Filters<R> = RowFilter<R>[];
type Row<R> = R & { ts_write: string };

type ServerMessage<R> = {
    table: string;
    row: Row<R>;
};

// The complete API runs over this WebSocket.
const socketConnection = webSocket<ServerMessage<unknown>>(
    "ws://localhost:8887"
);
socketConnection.pipe(retry({ delay: 1000 })).subscribe(() => {
    // Not sure why we need this, but otherwise the multiplex below does not
    // seem to connect correctly.
});
let nextSubscriptionId = 0;

class Table<R> {
    name: string;
    keys: (keyof R)[];
    filters?: Filters<R>;
    deps: unknown[] = [];

    constructor(name: string, keys: (keyof R)[]) {
        this.name = name;
        this.keys = keys;
    }

    acceptsRowWith(row: Row<R>, filters: RowFilter<R>) {
        for (const key in filters) {
            const filter = filters[key]!;
            if (Array.isArray(filter)) {
                if (!filter.includes(row[key])) {
                    return false;
                }
            } else {
                if (filter.start && filter.start > row[key]) {
                    return false;
                }
                if (filter.end && filter.end <= row[key]) {
                    return false;
                }
            }
        }
        return true;
    }

    acceptsRow(row: Row<R>) {
        if (!this.filters) {
            return true;
        } else {
            for (const filter of this.filters) {
                if (this.acceptsRowWith(row, filter)) {
                    return true;
                }
            }
            return false;
        }
    }

    acceptsMessage(message: ServerMessage<R>) {
        return message.table == this.name && this.acceptsRow(message.row);
    }

    where<C extends keyof R>(column: C, options: R[C][]): Table<R>;
    where<C extends keyof R>(column: C, start?: R[C], end?: R[C]): Table<R>;
    where<C extends keyof R>(column: C, start?: R[C] | R[C][], end?: R[C]) {
        let new_filters = this.filters;
        let new_deps = this.deps;
        if (!new_filters) {
            new_filters = [{}];
        }
        const last = new_filters?.splice(-1)![0];
        if (Array.isArray(start)) {
            new_filters = [...new_filters, { ...last, [column]: start }];
            new_deps = [...new_deps, ...start];
        } else {
            new_filters = [
                ...new_filters,
                { ...last, [column]: { start, end } },
            ];
            new_deps = [...new_deps, start, end];
        }
        const new_table = new Table(this.name, this.keys);
        new_table.filters = new_filters;
        new_table.deps = new_deps;
        return new_table;
    }

    or() {
        let new_filters = this.filters;
        if (!new_filters) {
            new_filters = [{}];
        } else {
            new_filters = [...new_filters, {}];
        }
        const new_table = new Table(this.name, this.keys);
        new_table.filters = new_filters;
        new_table.deps = this.deps;
        return new_table;
    }
}

export const countsLive = new Table<{
    window_size: string;
    kind: string;
    num_events: number;
}>("counts_live", ["window_size", "kind"]);

export function sortedKey<T, C>(
    fn: (a: T) => C,
    rev = false
): (a: T, b: T) => number {
    return (a, b) => {
        const [fa, fb] = [fn(a), fn(b)];
        const cmp = fa < fb ? -1 : fa > fb ? 1 : 0;
        return rev ? -cmp : cmp;
    };
}

export function sorted<T, C>(array: T[], fn: (a: T) => C, rev?: boolean): T[] {
    return array.toSorted(sortedKey(fn, rev));
}

export function groupKey<R>(row: R, keys: (keyof R)[]): string {
    return keys.map(k => row[k]).join(":");
}

export function groupBy<R>(table: R[], ...keys: (keyof R)[]): R[][] {
    return Object.values(
        Object.groupBy(table, row => groupKey(row, keys))
    ) as R[][];
}

export function useTable<R>(table: Table<R>): Row<R>[] | undefined;
export function useTable<R, T>(
    table: Table<R>,
    transform: (o: Row<R>[]) => T,
    deps?: unknown[]
): T | undefined;
export function useTable<R, T>(
    table: Table<R>,
    transform?: (o: Row<R>[]) => T,
    deps: unknown[] = []
): T | undefined {
    const viewRef = useRef(new Map<string, Row<R>>());
    const [subscribe, snapshot] = useMemo(() => {
        const view = viewRef.current;
        for (const [key, row] of [...view.entries()]) {
            if (!table.acceptsRow(row)) {
                view.delete(key);
            }
        }
        const subscriptionId = nextSubscriptionId++;
        const subscription = {
            id: subscriptionId,
            table: table.name,
            filters: table.filters,
        };
        const events = socketConnection
            .multiplex(
                () => ({
                    subscribe: [subscription],
                    replay: [subscription],
                }),
                () => ({ unsubscribe: [subscriptionId] }),
                message => table.acceptsMessage(message as ServerMessage<R>)
            )
            .pipe(
                retry({ delay: 1000 }),
                map(message => {
                    const row = message.row as Row<R>;
                    const rowKey = groupKey(row, table.keys);
                    const oldRow = view.get(rowKey);
                    if (!oldRow || oldRow.ts_write < row.ts_write) {
                        view.set(rowKey, row);
                        return true;
                    } else {
                        return false;
                    }
                }),
                filter(e => e),
                throttleTime(250)
            );
        let snapshot: T;
        return [
            (onChange: () => void) => {
                const subscription = events.subscribe(() => {
                    if (transform) {
                        snapshot = transform([...view.values()]);
                    } else {
                        snapshot = [...view.values()] as T;
                    }
                    onChange();
                });
                return () => subscription.unsubscribe();
            },
            () => snapshot,
        ];
        // Must be dynamic, since it is based on the filter we apply.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [...table.deps, ...deps]);
    return useSyncExternalStore(subscribe, snapshot);
}
