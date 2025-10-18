import type { WebSocketSubject } from "rxjs/webSocket";
import { auditTime, filter, map, retry } from "rxjs";

import { groupKey, sort } from "../util";
import {
    acceptsRowWith,
    getSubscriptionId,
    socketConnection,
    type Filter,
    type Filters,
    type InFilter,
    type RangeFilter,
    type Row,
    type RowMessage,
    type ServerMessage,
} from "./client";

export class Table<R> {
    name: string;
    keys: (keyof R)[];
    filters?: Filters<R>;
    limited?: number;
    deps: unknown[];

    constructor(
        name: string,
        keys: typeof this.keys,
        filters?: typeof this.filters,
        limited?: typeof this.limited,
        deps?: typeof this.deps
    ) {
        this.name = name;
        this.keys = keys;
        this.filters = filters;
        this.limited = limited;
        this.deps = deps ?? [];
    }

    clone(): this {
        return new Table(
            this.name,
            this.keys,
            this.filters,
            this.limited,
            this.deps
        ) as this;
    }

    acceptsMessage(message: RowMessage<R>) {
        return (
            message.table == this.name &&
            acceptsRowWith(message.row, this.filters)
        );
    }

    where<C extends keyof R>(column: C, options: InFilter<R[C]>): this;
    where<C extends keyof R>(column: C, range: RangeFilter<R[C]>): this;
    where<C extends keyof R>(column: C, filter: Filter<R[C]>) {
        let newFilters = this.filters;
        let newDeps = this.deps;
        if (!newFilters) {
            newFilters = [{}];
        }
        const last = newFilters?.splice(-1)![0];
        if (Array.isArray(filter)) {
            newFilters = [...newFilters, { ...last, [column]: filter }];
            newDeps = [...newDeps, ...filter];
        } else {
            newFilters = [...newFilters, { ...last, [column]: filter }];
            const { start, end, substr } = filter;
            newDeps = [...newDeps, start, end, substr];
        }
        const newTable = this.clone();
        newTable.filters = newFilters;
        newTable.deps = newDeps;
        return newTable;
    }

    limit(limit: number) {
        const newTable = this.clone();
        newTable.limited = limit;
        newTable.deps = [...this.deps, limit];
        return newTable;
    }

    or() {
        let newFilters = this.filters;
        if (!newFilters) {
            newFilters = [{}];
        } else {
            newFilters = [...newFilters, {}];
        }
        const newTable = this.clone();
        newTable.filters = newFilters;
        return newTable;
    }

    mergeRows(newRow: Row<R>, oldRow?: Row<R>) {
        if (!oldRow || newRow.seq_num > oldRow.seq_num) {
            return newRow;
        } else {
            return oldRow;
        }
    }

    filterView(view: Map<string, Row<R>>) {
        for (const [key, row] of [...view.entries()]) {
            if (!acceptsRowWith(row, this.filters)) {
                view.delete(key);
            }
        }
    }

    extractFromView(view: Map<string, Row<R>>) {
        this.filterView(view);
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

    mergeIntoView(view: Map<string, Row<R>>, newRow: Row<R>) {
        const rowKey = groupKey(newRow, this.keys);
        const oldRow = view.get(rowKey);
        const row = this.mergeRows(newRow, oldRow);
        if (row !== oldRow) {
            view.set(rowKey, row);
            return true;
        } else {
            return false;
        }
    }

    connect(view: Map<string, Row<R>>) {
        const subscriptionId = getSubscriptionId();
        const subscription = {
            id: subscriptionId,
            table: this.name,
            filters: this.filters,
            limit: this.limited,
        };
        let replayed = false;
        return (socketConnection as WebSocketSubject<ServerMessage<R>>)
            .multiplex(
                () => ({
                    subscribe: [subscription],
                    replay: [subscription],
                }),
                () => ({ unsubscribe: [subscriptionId] }),
                message =>
                    "row" in message
                        ? this.acceptsMessage(message)
                        : message.replayed == subscriptionId
            )
            .pipe(
                retry({ delay: 1000 }),
                map(message => {
                    if ("row" in message) {
                        return this.mergeIntoView(view, message.row);
                    } else {
                        replayed = true;
                        return true;
                    }
                }),
                filter(e => e),
                map(() => replayed),
                auditTime(50)
            );
    }
}

type UpdateRow<R> = R & { [P in keyof R as `${string & P}_seq_num`]: number };

export class UpdateTable<K, R> extends Table<K & UpdateRow<R>> {
    clone(): this {
        return new UpdateTable(
            this.name,
            this.keys as (keyof K & UpdateRow<R>)[],
            this.filters,
            this.limited,
            this.deps
        ) as this;
    }

    mergeRows(newRow: Row<K & UpdateRow<R>>, oldRow?: Row<K & UpdateRow<R>>) {
        if (oldRow) {
            const merged: Record<string, unknown> = { ...oldRow };
            for (const key in newRow) {
                if (!key.endsWith("_seq_num")) {
                    const seqKey = key + "_seq_num";
                    const newRowAny = newRow as Record<string, unknown>;
                    const oldRowAny = oldRow as Record<string, unknown>;
                    if (newRowAny[seqKey] != null) {
                        if (
                            (newRowAny[seqKey] as number) >
                            (oldRowAny[seqKey] as number)
                        ) {
                            merged[key] = newRowAny[key];
                            merged[seqKey] = newRowAny[seqKey];
                        }
                    } else {
                        merged[key] = newRowAny[key];
                    }
                }
            }
            return merged as Row<K & UpdateRow<R>>;
        } else {
            return newRow;
        }
    }
}
