import type { WebSocketSubject } from "rxjs/webSocket";
import { auditTime, filter, map, retry } from "rxjs";

import { groupKey } from "../util";
import {
    acceptsRowWith,
    getSubscriptionId,
    socketConnection,
    type RangeFilter,
    type Row,
    type RowFilter,
    type ServerMessage,
} from "./client";
import { Table } from "./table";

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
        const subscriptionId = getSubscriptionId();
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
                        for (const update of bufferedUpdates) {
                            applyUpdate(update);
                        }
                        bufferedUpdates.length = 0;
                        return true;
                    }
                }),
                filter(e => e),
                map(() => !!replaySeqNum),
                auditTime(50)
            );
    }
}
