import { webSocket, WebSocketSubject } from "rxjs/webSocket";
import { auditTime, filter, map, retry } from "rxjs/operators";
import { useMemo, useRef, useSyncExternalStore } from "react";
import { groupKey, sort } from "./util";

type InFilter<T> = T[];
type RangeFilter<T> = {
    start?: T;
    end?: T;
    substr?: T extends string ? T : undefined;
};

type Filter<T> = InFilter<T> | RangeFilter<T>;
type RowFilter<R> = { [P in keyof R]?: Filter<R[P]> };
type Filters<R> = RowFilter<R>[];
export type Row<R> = R & { seq_num: number };

type RowMessage<R> = {
    table: string;
    row: Row<R>;
};
type ReplayMessage = {
    table: string;
    replayed: number;
};

type ServerMessage<R> = ReplayMessage | RowMessage<R>;

type Subscription<R> = {
    id: number;
    table: string;
    filters?: Filters<R>;
    limit?: number;
};
type ClientMessage<R> = {
    subscribe?: Subscription<R>[];
    replay?: Subscription<R>[];
    unsubscribe?: number[];
};

// The complete API runs over this WebSocket.
const socketConnection = webSocket<
    ServerMessage<unknown> | ClientMessage<unknown>
>("ws://localhost:8887");
socketConnection.pipe(retry({ delay: 1000 })).subscribe(() => {
    // Not sure why we need this, but otherwise the multiplex below does not
    // seem to connect correctly. I assume there is some issue with the socket
    // connection being closed and opened in every rerender of the app.
});
// Subscriptions each get a unique id that can be used to unsubscribe them again.
let nextSubscriptionId = 0;

function acceptsRowWithOne<R>(row: Row<R>, filters: RowFilter<R>) {
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

function acceptsRowWith<R>(row: Row<R>, filters?: RowFilter<R>[]) {
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
        const subscriptionId = nextSubscriptionId++;
        const subscription = {
            id: subscriptionId,
            table: this.name,
            filters: this.filters,
            limit: this.limited,
        };
        return (socketConnection as WebSocketSubject<ServerMessage<R>>)
            .multiplex(
                () => ({
                    subscribe: [subscription],
                    replay: [subscription],
                }),
                () => ({ unsubscribe: [subscriptionId] }),
                message => "row" in message && this.acceptsMessage(message)
            )
            .pipe(
                retry({ delay: 1000 }),
                map(message => {
                    return (
                        "row" in message &&
                        this.mergeIntoView(view, message.row)
                    );
                }),
                filter(e => e),
                auditTime(50)
            );
    }
}

type UpdateRow<R> = R & { [P in keyof R as `${string & P}_seq_num`]: number };

export class UpdateTable<K, R> extends Table<K & UpdateRow<R>> {
    clone(): this {
        return new UpdateTable(
            this.name,
            this.keys,
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

export type RankingRow<R> = R & { row_number: number; rank: number };
type RankingUpdateRow<R> = R &
    (
        | {
              row_number: number;
              rank: number;
              max_rank: number;
          }
        | {
              row_number: null;
              rank: null;
              max_rank: null;
          }
    ) &
    (
        | {
              old_row_number: number;
              old_rank: number;
              old_max_rank: number;
          }
        | {
              old_row_number: null;
              old_rank: null;
              old_max_rank: null;
          }
    );
type RankingMsgRow<R> = RankingRow<R> | RankingUpdateRow<R>;

const MARGIN = 50;

export class RankingTable<R> extends Table<RankingRow<R>> {
    rankingKeys: (keyof R)[] = [];
    range?: [number, number];

    clone(): this {
        const table = new RankingTable(
            this.name,
            this.keys,
            this.filters,
            this.limited,
            this.deps
        ) as this;
        table.range = this.range;
        table.rankingKeys = this.rankingKeys;
        return table;
    }

    // How are different rankings separated in the table.
    rankingsBy(keys: (keyof R)[]) {
        const newTable = this.clone();
        newTable.rankingKeys = keys;
        return newTable;
    }

    desiredRows(start: number, end: number) {
        const newTable = this.where("row_number", {
            start: start - MARGIN,
            end: end + MARGIN,
        } as RangeFilter<RankingRow<R>["row_number"]>);
        newTable.range = [start, end];
        return newTable;
    }

    filterView(view: Map<string, Row<RankingRow<R>>>) {
        const newestPerRow = new Map<number, number>();
        for (const row of view.values()) {
            if ((newestPerRow.get(row.row_number) ?? 0) < row.seq_num) {
                newestPerRow.set(row.row_number, row.seq_num);
            }
        }
        for (const [key, row] of [...view.entries()]) {
            if (
                row.seq_num < (newestPerRow.get(row.row_number) ?? 0) ||
                !acceptsRowWith(row, this.filters)
            ) {
                view.delete(key);
            }
        }
    }

    extractFromView(view: Map<string, Row<RankingRow<R>>>) {
        if (this.range) {
            return (
                super
                    .extractFromView(view)
                    // Restrict only to the range we are actually interested in. We
                    // keep more in the view to account for shifting in the ranks.
                    .filter(
                        row =>
                            row.row_number >= this.range![0] &&
                            row.row_number < this.range![1]
                    )
            );
        } else {
            return super.extractFromView(view);
        }
    }

    extendSubscriptionFilters(filters?: RowFilter<Row<RankingUpdateRow<R>>>[]) {
        if (!filters || !this.range) {
            return filters;
        }
        const newFilters = [];
        for (const filter of filters) {
            // Only the elements above us in the ranking can affect the range.
            // This means either the old or new row numbers must be above our
            // range for the event to be of interest to us.
            newFilters.push({
                ...filter,
                row_number: { end: this.range[1] + MARGIN },
            });
            newFilters.push({
                ...filter,
                old_row_number: { end: this.range[1] + MARGIN },
            });
        }
        return newFilters;
    }

    inExtendedFilters(row: number) {
        // Basically whenever a tracked row leaves this range, we might have missed
        // events needed to correctly update it. For this reason, these rows should
        // no longer be considered.
        return !this.range || row < this.range[1] + MARGIN;
    }

    connect(view: Map<string, Row<RankingRow<R>>>) {
        const subscriptionId = nextSubscriptionId++;
        const subscriptionFilter = this.filters?.map(f =>
            Object.fromEntries(
                Object.entries(f).filter(([name, _filter]) =>
                    (this.rankingKeys as string[]).includes(name)
                )
            )
        ) as RowFilter<Row<RankingUpdateRow<R>>>[] | undefined;
        const subscription = {
            id: subscriptionId,
            table: this.name,
            filters: this.extendSubscriptionFilters(subscriptionFilter),
        };
        const replay = {
            id: subscriptionId,
            table: this.name,
            filters: this.filters,
            limit: this.limited,
        };
        let replaySeqNum: number | undefined = undefined;
        const safeRanges = new Map<string, [number, number]>();
        const bufferedUpdates: Row<RankingUpdateRow<R>>[] = [];
        // Applies the update from the ranking update stream to the given range.
        const applyUpdateToRange = (
            [a, b]: [number, number],
            update: Row<RankingUpdateRow<R>>
        ): [number, number] => {
            if (update.old_row_number !== null) {
                if (a > update.old_row_number) {
                    a -= 1;
                }
                if (b >= update.old_row_number) {
                    b -= 1;
                }
            }
            if (update.row_number !== null) {
                if (a >= update.row_number) {
                    a += 1;
                }
                if (b > update.row_number) {
                    b += 1;
                }
            }
            return [
                // Bound by the range we actually look at. We delete those outside.
                Math.max(a, this.range![0] - MARGIN),
                Math.min(b, this.range![1] + MARGIN),
            ];
        };
        // Applies the update from the ranking update stream to all rows in the view.
        // This function will also delete all elements outside the range.
        const applyUpdateToView = (update: Row<RankingUpdateRow<R>>) => {
            const key = groupKey(update, this.keys);
            if (update.old_row_number !== null) {
                if (view.has(key)) {
                    const oldRow = view.get(key)!;
                    if (this.inExtendedFilters(oldRow.row_number)) {
                        if (oldRow.row_number != update.old_row_number) {
                            console.warn("found inconsistent row numbers");
                            return false;
                        }
                        if (oldRow.rank != update.old_rank) {
                            console.warn("found inconsistent ranks");
                            return false;
                        }
                    }
                    view.delete(key);
                }
                for (const row of view.values()) {
                    if (
                        this.rankingKeys.every(k => row[k] == update[k]) &&
                        this.inExtendedFilters(row.row_number)
                    ) {
                        if (row.row_number >= update.old_max_rank) {
                            row.rank -= 1;
                        }
                        if (row.row_number == update.old_row_number) {
                            console.warn(
                                "found row number that should have been removed"
                            );
                            return false;
                        }
                        if (row.row_number > update.old_row_number) {
                            row.row_number -= 1;
                        }
                    }
                }
            }
            if (update.row_number !== null) {
                for (const row of view.values()) {
                    if (
                        this.rankingKeys.every(k => row[k] == update[k]) &&
                        this.inExtendedFilters(row.row_number)
                    ) {
                        if (row.row_number >= update.row_number) {
                            row.row_number += 1;
                        }
                        if (row.row_number >= update.max_rank) {
                            row.rank += 1;
                        }
                    }
                }
                view.set(key, update as Row<RankingRow<R>>);
            }
            return true;
        };
        // Replay again to make sure the client view is correct.
        const replayAgain = () => {
            replaySeqNum = undefined;
            bufferedUpdates.length = 0;
            safeRanges.clear();
            socketConnection.next({
                replay: [replay],
            });
        };
        // Consumes an update, updates the range and view, and if required will issue
        // new replay requests to the server to get the up-to-date information.
        const applyUpdate = (update: Row<RankingUpdateRow<R>>) => {
            if (update.seq_num > replaySeqNum!) {
                if (this.range) {
                    const rankingKey = groupKey(update, this.rankingKeys);
                    if (!safeRanges.has(rankingKey)) {
                        safeRanges.set(rankingKey, [
                            this.range[0] - MARGIN,
                            this.range[1] + MARGIN,
                        ]);
                    }
                    const newRange = applyUpdateToRange(
                        safeRanges.get(rankingKey)!,
                        update
                    );
                    if (
                        newRange[0] > this.range![0] ||
                        newRange[1] < this.range![1]
                    ) {
                        // Our initial elements no longer cover the desired range.
                        // We have to request more elements by again replaying the
                        // initial extended range.
                        replayAgain();
                        return false;
                    } else {
                        safeRanges.set(rankingKey, newRange);
                    }
                }
                if (!applyUpdateToView(update)) {
                    // Something went wrong. Better to rehydrate from a replay.
                    replayAgain();
                    return false;
                }
                return true;
            } else {
                return false;
            }
        };
        return (
            socketConnection as WebSocketSubject<
                ServerMessage<RankingMsgRow<R>>
            >
        )
            .multiplex(
                () => ({
                    subscribe: [subscription],
                    replay: [replay],
                }),
                () => ({ unsubscribe: [subscriptionId] }),
                message =>
                    message.table == this.name &&
                    ("row" in message
                        ? "old_row_number" in message.row
                            ? acceptsRowWith(message.row, subscriptionFilter)
                            : acceptsRowWith(message.row, this.filters)
                        : message.replayed == subscriptionId)
            )
            .pipe(
                retry({ delay: 1000 }),
                map(message => {
                    if ("row" in message) {
                        if ("old_row_number" in message.row) {
                            if (replaySeqNum) {
                                return applyUpdate(message.row);
                            } else {
                                bufferedUpdates.push(message.row);
                                return false;
                            }
                        } else {
                            view.set(
                                groupKey(message.row, this.keys),
                                message.row
                            );
                            return true;
                        }
                    } else {
                        replaySeqNum = [...view.values()]
                            .map(r => r.seq_num)
                            .reduce((a, b) => Math.max(a, b), 1);
                        // Filter the view to get rid of the old replays records.
                        this.filterView(view);
                        let some = false;
                        for (const update of bufferedUpdates) {
                            some = applyUpdate(update) || some;
                        }
                        bufferedUpdates.length = 0;
                        return some;
                    }
                }),
                filter(e => e),
                auditTime(50)
            );
    }
}

export function useTable<R>(table: Table<R>): Row<R>[] {
    // The view keeps rows from the table between different connections.
    const viewRef = useRef(new Map<string, Row<R>>());
    const [subscribe, snapshot] = useMemo(() => {
        const view = viewRef.current;
        const events = table.connect(view);
        // Initial build of snapshot reusing rows that we already know about.
        let snapshot = table.extractFromView(view);
        return [
            (onChange: () => void) => {
                const subscription = events.subscribe(() => {
                    snapshot = table.extractFromView(view);
                    onChange();
                });
                return () => subscription.unsubscribe();
            },
            () => snapshot,
        ];
        // Must be dynamic, since it is based on the filter we apply.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [...table.deps]);
    return useSyncExternalStore(subscribe, snapshot);
}
