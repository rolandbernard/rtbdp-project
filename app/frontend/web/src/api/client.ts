import { webSocket } from "rxjs/webSocket";
import { auditTime, filter, map, retry } from "rxjs/operators";
import { useMemo, useRef, useSyncExternalStore } from "react";
import { groupKey, sort } from "./util";

type Filter<T> =
    | T[]
    | { start?: T; end?: T; substr?: T extends string ? T : undefined };
type RowFilter<R> = { [P in keyof R]?: Filter<R[P]> };
type Filters<R> = RowFilter<R>[];
type Row<R> = R & { seq_num: number };

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
    // seem to connect correctly. I assume there is some issue with the socket
    // connection being closed and opened in every rerender of the app.
});
// Subscriptions each get a unique id that can be used to unsubscribe them again.
let nextSubscriptionId = 0;

export class Table<R> {
    name: string;
    keys: (keyof R)[];
    filters?: Filters<R>;
    limited?: number;
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
                if (
                    filter.substr &&
                    !(row[key] as string).toLowerCase().includes(filter.substr)
                ) {
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

    applyLimiting(view: Map<string, Row<R>>) {
        if (this.limited && view.size > this.limited) {
            const sorted = sort(
                [...view.entries()],
                this.keys.map(
                    k =>
                        ([_key, row]) =>
                            row[k]
                ),
                true
            );
            for (const [key, _row] of sorted.splice(this.limited)) {
                view.delete(key);
            }
            return sorted.map(([_key, row]) => row);
        } else {
            return [...view.values()];
        }
    }

    where<C extends keyof R>(column: C, options: R[C][]): Table<R>;
    where<C extends keyof R>(
        column: C,
        range: { start?: R[C]; end?: R[C] }
    ): Table<R>;
    where<C extends keyof R>(column: C, filter: Filter<R[C]>) {
        let new_filters = this.filters;
        let new_deps = this.deps;
        if (!new_filters) {
            new_filters = [{}];
        }
        const last = new_filters?.splice(-1)![0];
        if (Array.isArray(filter)) {
            new_filters = [...new_filters, { ...last, [column]: filter }];
            new_deps = [...new_deps, ...filter];
        } else {
            new_filters = [...new_filters, { ...last, [column]: filter }];
            const { start, end, substr } = filter;
            new_deps = [...new_deps, start, end, substr];
        }
        const new_table = new Table(this.name, this.keys);
        new_table.filters = new_filters;
        new_table.limited = this.limited;
        new_table.deps = new_deps;
        return new_table;
    }

    limit(limit: number) {
        const new_table = new Table(this.name, this.keys);
        new_table.filters = this.filters;
        new_table.limited = limit;
        new_table.deps = this.deps;
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
        new_table.limited = this.limited;
        new_table.deps = this.deps;
        return new_table;
    }
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
            limit: table.limited,
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
                    if (!oldRow || oldRow.seq_num < row.seq_num) {
                        view.set(rowKey, row);
                        return true;
                    } else {
                        return false;
                    }
                }),
                filter(e => e),
                auditTime(50)
            );
        let snapshot: T;
        return [
            (onChange: () => void) => {
                const subscription = events.subscribe(() => {
                    const values = table.applyLimiting(view);
                    if (transform) {
                        snapshot = transform(values);
                    } else {
                        snapshot = values as T;
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
