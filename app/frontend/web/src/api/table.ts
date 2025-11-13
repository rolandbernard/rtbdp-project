import type { WebSocketSubject } from "rxjs/webSocket";
import {
    auditTime,
    combineLatest,
    filter,
    from,
    map,
    Observable,
    retry,
    startWith,
} from "rxjs";

import { groupKey, sort } from "../util";
import {
    acceptsRowWith,
    getSubscriptionId,
    getConnection,
    type Filter,
    type Filters,
    type InFilter,
    type RangeFilter,
    type Row,
    type RowMessage,
    type ServerMessage,
} from "./client";

export abstract class Table<R, V> {
    abstract createView(): V;

    abstract extractFromView(view: V): Row<R>[];

    abstract connect(view: V): Observable<boolean>;

    abstract dependencies(): unknown[];
}

export class NormalTable<R> extends Table<R, Map<string, Row<R>>> {
    name: string;
    keys: (keyof R)[];
    filters?: Filters<R>;
    limited?: number;
    deps: unknown[];

    constructor(
        name: string,
        keys: (keyof R)[],
        filters?: Filters<R>,
        limited?: number,
        deps?: unknown[]
    ) {
        super();
        this.name = name;
        this.keys = keys;
        this.filters = filters;
        this.limited = limited;
        this.deps = deps ?? [name];
    }

    clone(): this {
        return new NormalTable(
            this.name,
            this.keys,
            this.filters,
            this.limited,
            this.deps
        ) as this;
    }

    createView(): Map<string, Row<R>> {
        return new Map();
    }

    dependencies(): unknown[] {
        return this.deps;
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
        const newFilters = [...(this.filters ?? [{}])];
        const last = newFilters?.splice(-1)![0];
        newFilters.push({ ...last, [column]: filter });
        let newDeps = this.deps;
        if (Array.isArray(filter)) {
            newDeps = [...newDeps, ...filter];
        } else {
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
            if (
                !key.startsWith(this.name + ":") ||
                !acceptsRowWith(row, this.filters)
            ) {
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
                            row[k] as number | string
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

    groupKey(row: Row<R>) {
        return this.name + ":" + groupKey(row, this.keys);
    }

    mergeIntoView(view: Map<string, Row<R>>, newRow: Row<R>) {
        const rowKey = this.groupKey(newRow);
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
        return (getConnection() as WebSocketSubject<ServerMessage<R>>)
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
                        if (message.rows) {
                            message.rows.seq_num.forEach((_, i) => {
                                this.mergeIntoView(
                                    view,
                                    Object.fromEntries(
                                        Object.keys(message.rows!).map(k => [
                                            k,
                                            message.rows![k as keyof Row<R>][i],
                                        ])
                                    ) as Row<R>
                                );
                            });
                        }
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

export class UpdateTable<K, R> extends NormalTable<K & UpdateRow<R>> {
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

export class UnionTable<
    V extends unknown[],
    R extends { [K in keyof V]: unknown }
> extends Table<R[keyof V], V> {
    tables: { [K in keyof V]: Table<R[K], V[K]> };

    constructor(tables: { [K in keyof V]: Table<R[K], V[K]> }) {
        super();
        this.tables = tables;
    }

    createView(): V {
        return this.tables.map(table => table.createView()) as V;
    }

    extractFromView(view: V): Row<R[keyof V]>[] {
        return this.tables.flatMap((table, i) =>
            table.extractFromView(view[i])
        );
    }

    connect(view: V): Observable<boolean> {
        return combineLatest(
            this.tables.map((table, i) =>
                table.connect(view[i]).pipe(startWith(false))
            )
        ).pipe(
            map(values => values.every(e => e)),
            auditTime(50)
        );
    }

    dependencies(): unknown[] {
        return this.tables.flatMap(table => table.dependencies());
    }
}

export class ConstantTable<R> extends Table<R, void> {
    values: R[];

    constructor(values: R[]) {
        super();
        this.values = values;
    }

    createView() {
        return undefined;
    }

    extractFromView(): Row<R>[] {
        return this.values.map(row => ({
            ...row,
            seq_num: 0,
        }));
    }

    connect(): Observable<boolean> {
        return from([true]);
    }

    dependencies(): unknown[] {
        return [this.values];
    }
}
