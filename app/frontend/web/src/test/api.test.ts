import { describe, it, expect, beforeEach, vi } from "vitest";

import {
    acceptsRowWithOne,
    acceptsRowWith,
    type Row,
    type RowFilter,
    type Filters,
} from "../api/client";

describe("getSubscriptionId", () => {
    beforeEach(() => {
        vi.resetModules();
    });

    it("returns incrementing IDs", async () => {
        const client = await import("../api/client");

        const id1 = client.getSubscriptionId();
        const id2 = client.getSubscriptionId();
        const id3 = client.getSubscriptionId();

        expect(id1).toBe(0);
        expect(id2).toBe(1);
        expect(id3).toBe(2);
    });
});

describe("acceptsRowWithOne", () => {
    type TestRow = Row<{ name: string; age: number; active: boolean }>;

    it("accepts row when it passes all filters", () => {
        const row: TestRow = {
            seq_num: 1,
            name: "Alice",
            age: 25,
            active: true,
        };
        const filters: RowFilter<TestRow> = {
            name: ["Alice", "Bob"],
            age: { start: 20, end: 30 },
            active: [true],
        };
        expect(acceptsRowWithOne(row, filters)).toBe(true);
    });

    it("rejects row when it fails any filter", () => {
        const row: TestRow = {
            seq_num: 1,
            name: "Charlie",
            age: 25,
            active: true,
        };
        const filters: RowFilter<TestRow> = {
            name: ["Alice", "Bob"],
            age: { start: 20, end: 30 },
            active: [true],
        };
        expect(acceptsRowWithOne(row, filters)).toBe(false);
    });

    it("handles InFilter (array)", () => {
        const row: TestRow = {
            seq_num: 1,
            name: "Alice",
            age: 25,
            active: true,
        };
        expect(acceptsRowWithOne(row, { name: ["Alice", "Bob"] })).toBe(true);
        expect(acceptsRowWithOne(row, { name: ["Bob", "Charlie"] })).toBe(
            false
        );
    });

    it("handles RangeFilter with start and end", () => {
        const row: TestRow = {
            seq_num: 1,
            name: "Alice",
            age: 25,
            active: true,
        };
        const filters: RowFilter<TestRow> = {
            name: ["Alice", "Bob"],
            age: { start: 20, end: 30 },
            active: [true],
        };
        expect(acceptsRowWithOne(row, filters)).toBe(true);
    });

    it("handles RangeFilter with substr", () => {
        const row: TestRow = {
            seq_num: 1,
            name: "Alice",
            age: 25,
            active: true,
        };
        expect(acceptsRowWithOne(row, { name: { substr: "lic" } })).toBe(true);
        expect(acceptsRowWithOne(row, { name: { substr: "xyz" } })).toBe(false);
        expect(acceptsRowWithOne(row, { name: { substr: "ALI" } })).toBe(false);
        expect(acceptsRowWithOne(row, { name: { substr: "ali" } })).toBe(true);
    });

    it("handles partial RangeFilter", () => {
        const row: TestRow = {
            seq_num: 1,
            name: "Alice",
            age: 25,
            active: true,
        };
        expect(acceptsRowWithOne(row, { age: { start: 20 } })).toBe(true);
        expect(acceptsRowWithOne(row, { age: { start: 30 } })).toBe(false);
        expect(acceptsRowWithOne(row, { age: { end: 30 } })).toBe(true);
        expect(acceptsRowWithOne(row, { age: { end: 20 } })).toBe(false);
    });
});

describe("acceptsRowWith", () => {
    type TestRow = Row<{ name: string; age: number; active: boolean }>;

    it("accepts row when no filters provided", () => {
        const row: TestRow = {
            seq_num: 1,
            name: "Alice",
            age: 25,
            active: true,
        };
        expect(acceptsRowWith(row, undefined)).toBe(true);
        expect(acceptsRowWith(row, [])).toBe(false);
    });

    it("accepts row when it passes any filter group", () => {
        const row: TestRow = {
            seq_num: 1,
            name: "Alice",
            age: 25,
            active: true,
        };
        const filters: Filters<TestRow> = [
            { name: ["Bob", "Charlie"] },
            { age: { start: 20, end: 30 } },
            { active: [false] },
        ];
        expect(acceptsRowWith(row, filters)).toBe(true);
    });

    it("rejects row when it fails all filter groups", () => {
        const row: TestRow = {
            seq_num: 1,
            name: "Alice",
            age: 25,
            active: true,
        };
        const filters: Filters<TestRow> = [
            { name: ["Bob", "Charlie"] },
            { age: { start: 30, end: 40 } },
            { active: [false] },
        ];

        expect(acceptsRowWith(row, filters)).toBe(false);
    });
});
