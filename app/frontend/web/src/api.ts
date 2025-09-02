
import { webSocket } from 'rxjs/webSocket';
import { retry, map, tap } from 'rxjs/operators';
import { useSyncExternalStore } from 'react';

// The complete API runs over this WebSocket.
const socketConnection = webSocket("ws://localhost:8887");
let nextSubscriptionId = 0;

type Filter = (string | number)[]
    | { start?: number | string, end?: number | string, inclusive?: boolean };

type Filters = Record<string, Filter>[];

function acceptsRow(row: any, filters: Record<string, Filter>) {
    for (const key in filters) {
        
        if (Array.isArray(filters[key])) {
            if (!filters[key].includes(row[key])) {
                return false;
            }
        } else {
            // if ()
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

export function useQuery(table: string, key: string, filters?: Filters) {
    const subscriptionId = nextSubscriptionId++;
    const events = socketConnection.multiplex(
        () => ({ }),
        () => ({ unsubscribe: [subscriptionId] }),
        message => acceptsMessage(message, table, filters));
    return useSyncExternalStore();
}

