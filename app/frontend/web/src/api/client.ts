import { webSocket, WebSocketSubject } from "rxjs/webSocket";
import { catchError, retry } from "rxjs/operators";

export type Row<R> = R & { seq_num: number };
export type MultiRow<R> = { [K in keyof Row<R>]: Row<R>[K][] };
export type RowMessage<R> = {
    table: string;
    row?: Row<R>;
    rows?: MultiRow<R>;
};
export type ReplayMessage = {
    replayed: number;
};
export type ServerMessage<R> = ReplayMessage | RowMessage<R>;

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
type Connection = WebSocketSubject<
    ServerMessage<unknown> | ClientMessage<unknown>
>;

function getFreshConnection() {
    // Determine the URL that the API will be available at.
    let url;
    const params = new URLSearchParams(document.location.search);
    if (params.has("api")) {
        url = params.get("api")!;
    } else {
        if (
            document.location.port == "8888" ||
            document.location.hostname == "localhost"
        ) {
            url = `ws://${document.location.hostname}:8887`;
        } else {
            const proto = document.location.protocol == "https:" ? "wss" : "ws";
            url = `${proto}://${document.location.host}/api`;
        }
    }
    return webSocket<ServerMessage<unknown> | ClientMessage<unknown>>(url);
}

function getAndStartConnection() {
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
                        loginUrl + "?url=" + encodeURIComponent(location.href);
                } else {
                    console.error(e);
                }
            }),
            retry({ delay: 1000 }),
        )
        .subscribe(() => {
            // We need this to keep the connection constantly open, instead
            // of opening and closing possible whenever there are temporarily
            // no users between renders.
        });
    return connection;
}

// We create up to eight connection for better parallelism.
const MAX_CONNECTIONS = 3;
const connectionPool: Connection[] = [];
const dedicatedConnections = new Map<string, Connection>();
let nextConnection = 0;

export function getConnection(dedicated?: string) {
    if (dedicated) {
        if (!dedicatedConnections.has(dedicated)) {
            dedicatedConnections.set(dedicated, getAndStartConnection());
        }
        return dedicatedConnections.get(dedicated)!;
    } else {
        if (connectionPool.length < MAX_CONNECTIONS) {
            connectionPool.push(getAndStartConnection());
        }
        const connection = connectionPool[nextConnection]!;
        nextConnection = (nextConnection + 1) % MAX_CONNECTIONS;
        return connection;
    }
}

// Subscriptions each get a unique id that can be used to unsubscribe them again.
let nextSubscriptionId = 0;

export function getSubscriptionId() {
    const res = nextSubscriptionId;
    nextSubscriptionId++;
    return res;
}

export type Filter<T> = {
    opt?: T[];
    start?: T;
    end?: T;
    substr?: T extends string ? T : undefined;
};
export type RowFilter<R> = { [P in keyof R]?: Filter<R[P]> };
export type Filters<R> = RowFilter<R>[];

export function acceptsRowWithOne<R>(row: Row<R>, filters: RowFilter<R>) {
    for (const key in filters) {
        const filter = filters[key]!;
        if (filter.opt && !filter.opt.includes(row[key])) {
            return false;
        }
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
