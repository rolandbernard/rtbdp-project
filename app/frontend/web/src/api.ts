import { webSocket } from "rxjs/webSocket";
import { retry, map, tap } from "rxjs/operators";
import { useMemo, useRef, useSyncExternalStore } from "react";

// The complete API runs over this WebSocket.
const socketConnection = webSocket("ws://localhost:8887");
socketConnection.pipe(retry({ delay: 1000 })).subscribe(_m => {
    // Not sure why we need this, but otherwise the multiplex below does not
    // seem to connect correctly.
});
let nextSubscriptionId = 0;

type Row = Record<string, any> & { ts_write: string };

type Filter =
    | (string | number)[]
    | { start?: number | string; end?: number | string };

type Filters = Record<string, Filter>[];

function acceptsRow(row: any, filters: Record<string, Filter>) {
    for (const key in filters) {
        if (Array.isArray(filters[key])) {
            if (!filters[key].includes(row[key])) {
                return false;
            }
        } else {
            if (filters.start && filters.start > row[key]) {
                return false;
            }
            if (filters.end && filters.end <= row[key]) {
                return false;
            }
        }
    }
    return true;
}

function acceptsMessage(message: any, table: string, filters?: Filters) {
    if (message.table !== table) {
        return false;
    } else if (!filters) {
        return true;
    } else {
        for (const filter of filters) {
            if (acceptsRow(message.row, filter)) {
                return true;
            }
        }
        return false;
    }
}

export function useTable(
    table: string,
    keys: string[],
    deps: any[],
    filters?: Filters
) {
    const viewRef = useRef({} as Record<string, Row>);
    const [subscribe, snapshot] = useMemo(() => {
        const view = viewRef.current;
        const subscriptionId = nextSubscriptionId++;
        const subscription = { id: subscriptionId, table: table, filters };
        const events = socketConnection
            .multiplex(
                () => ({
                    subscribe: [subscription],
                    replay: [subscription],
                }),
                () => ({ unsubscribe: [subscriptionId] }),
                message => acceptsMessage(message, table, filters)
            )
            .pipe(retry({ delay: 1000 }));
        let snapshot: Row[] = [];
        return [
            (onChange: () => void) => {
                const subscription = events.subscribe((message: any) => {
                    const row: Row = message.row;
                    const rowKey = keys.map(k => row[k]).join(":");
                    const oldRow = view[rowKey];
                    if (!oldRow || oldRow.ts_write < row.ts_write) {
                        view[rowKey] = row;
                        snapshot = Object.values(view);
                        onChange();
                    }
                });
                return () => subscription.unsubscribe();
            },
            () => snapshot,
        ];
    }, deps);
    return useSyncExternalStore(subscribe, snapshot);
}
