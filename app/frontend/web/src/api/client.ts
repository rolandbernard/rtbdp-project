import { webSocket, WebSocketSubject } from "rxjs/webSocket";
import { catchError, retry } from "rxjs/operators";

export type Row<R> = R & { seq_num: number };
export type MultiRow<R> = { [K in keyof Row<R>]: Row<R>[K][] };
export type RowMessage<R> = {
    table: string;
    row: Row<R>;
};
export type ReplayMessage<R> = {
    replayed: number;
    rows?: MultiRow<R>;
};
export type ServerMessage<R> = ReplayMessage<R> | RowMessage<R>;

export type Subscription<R> = {
    id: number;
    table: string;
    filters?: Filters<R>;
    limit?: number;
};
export type ClientMessage<R> = {
    subscribe?: Subscription<R>[];
    replay?: Subscription<R>[];
    unsubscribe?: number[];
};

export function getFreshConnection() {
    // Determine the URL that the API will be available at.
    let url;
    if (document.location.hostname == "localhost") {
        url = "ws://localhost:8887";
    } else {
        url =
            (document.location.protocol == "https:" ? "wss://" : "ws://") +
            document.location.host +
            "/api";
    }
    return webSocket<ServerMessage<unknown> | ClientMessage<unknown>>(url);
}

// We create up to eight connection for better parallelism.
const MAX_CONNECTIONS = 4;
const connectionPool: WebSocketSubject<
    ServerMessage<unknown> | ClientMessage<unknown>
>[] = [];
let nextConnection = 0;

export function getConnection() {
    if (connectionPool.length < MAX_CONNECTIONS) {
        const connection = getFreshConnection();
        // Determine the URL to login at.
        let loginUrl = "/login";
        if (document.location.hostname == "localhost") {
            loginUrl = "http://localhost:8888/login";
        }
        connection
            .pipe(
                // @ts-expect-error Wrong type signature in rxjs.
                catchError(e => {
                    if (e.reason === "missing auth") {
                        location.href =
                            loginUrl +
                            "?url=" +
                            encodeURIComponent(location.href);
                    }
                }),
                retry({ delay: 1000 })
            )
            .subscribe(() => {
                // We need this to keep the connection constantly open, instead
                // of opening and closing possible whenever there are temporarily
                // no users between renders.
            });
        connectionPool.push(connection);
    }
    const connection = connectionPool[nextConnection]!;
    nextConnection = (nextConnection + 1) % MAX_CONNECTIONS;
    return connection;
}

// Subscriptions each get a unique id that can be used to unsubscribe them again.
let nextSubscriptionId = 0;

export function getSubscriptionId() {
    const res = nextSubscriptionId;
    nextSubscriptionId++;
    return res;
}

export type InFilter<T> = T[];
export type RangeFilter<T> = {
    start?: T;
    end?: T;
    substr?: T extends string ? T : undefined;
};

export type Filter<T> = InFilter<T> | RangeFilter<T>;
export type RowFilter<R> = { [P in keyof R]?: Filter<R[P]> };
export type Filters<R> = RowFilter<R>[];

export function acceptsRowWithOne<R>(row: Row<R>, filters: RowFilter<R>) {
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
                !(
                    row[key] &&
                    (row[key] as string).toLowerCase().includes(filter.substr)
                )
            ) {
                return false;
            }
        }
    }
    return true;
}

export function acceptsRowWith<R>(row: Row<R>, filters?: RowFilter<R>[]) {
    if (!filters) {
        return true;
    } else {
        for (const filter of filters) {
            if (acceptsRowWithOne(row, filter)) {
                return true;
            }
        }
        return false;
    }
}
