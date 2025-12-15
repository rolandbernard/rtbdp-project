import { describe, it, expect } from "vitest";

import {
    sortedKey,
    groupKey,
    groupBy,
    formatDate,
    findTicks,
    colorFor,
    toSorted,
} from "../util";

describe("sortedKey", () => {
    it("creates a comparator for a single key", () => {
        const users = [
            { name: "Alice", age: 30 },
            { name: "Bob", age: 25 },
        ];
        const compare = sortedKey<User>([(u: User) => u.age]);
        expect(compare(users[0]!, users[1]!)).toBe(1);
        expect(compare(users[1]!, users[0]!)).toBe(-1);
        expect(compare(users[0]!, users[0]!)).toBe(0);
    });

    it("creates a reversible comparator", () => {
        const users = [
            { name: "Alice", age: 30 },
            { name: "Bob", age: 25 },
        ];
        const compare = sortedKey<User>([(u: User) => u.age], true);
        expect(compare(users[0]!, users[1]!)).toBe(-1);
    });

    it("creates a comparator for multiple keys", () => {
        const users = [
            { name: "Alice", age: 25 },
            { name: "Bob", age: 25 },
            { name: "Charlie", age: 30 },
        ];
        const compare = sortedKey<User>([
            (u: User) => u.age,
            (u: User) => u.name,
        ]);
        expect(compare(users[0]!, users[1]!)).toBe(-1);
        expect(compare(users[0]!, users[2]!)).toBe(-1);
    });
});

type User = { name: string; age: number };

describe("sort", () => {
    it("sorts an array using the comparator", () => {
        const users = [
            { name: "Charlie", age: 30 },
            { name: "Alice", age: 25 },
            { name: "Bob", age: 25 },
        ];
        const sorted = toSorted(users, [(u: User) => u.age, (u: User) => u.name]);
        expect(sorted).toEqual([
            { name: "Alice", age: 25 },
            { name: "Bob", age: 25 },
            { name: "Charlie", age: 30 },
        ]);
    });

    it("sorts in reverse when specified", () => {
        const users = [
            { name: "Alice", age: 25 },
            { name: "Bob", age: 25 },
            { name: "Charlie", age: 30 },
        ];
        const sorted = toSorted(users, [(u: User) => u.age], true);
        expect(sorted).toEqual([
            { name: "Charlie", age: 30 },
            { name: "Alice", age: 25 },
            { name: "Bob", age: 25 },
        ]);
    });
});

describe("groupKey", () => {
    it("creates a unique key from row values", () => {
        const row = { name: "Alice", age: 25, city: "NYC" };
        expect(groupKey(row, ["name", "age"])).toBe("Alice:25");
        expect(groupKey(row, ["city"])).toBe("NYC");
    });
});

describe("groupBy", () => {
    it("groups rows by specified keys", () => {
        const data = [
            { name: "Alice", age: 25, city: "NYC" },
            { name: "Bob", age: 25, city: "NYC" },
            { name: "Charlie", age: 30, city: "NYC" },
            { name: "David", age: 25, city: "LA" },
        ];
        const grouped = groupBy(data, "age");
        expect(grouped).toHaveLength(2);
        const age25Group = grouped.find(group => group[0]?.age === 25);
        const age30Group = grouped.find(group => group[0]?.age === 30);
        expect(age25Group).toHaveLength(3);
        expect(age30Group).toHaveLength(1);
    });

    it("groups by multiple keys", () => {
        const data = [
            { name: "Alice", age: 25, city: "NYC" },
            { name: "Bob", age: 25, city: "LA" },
            { name: "Charlie", age: 25, city: "NYC" },
        ];
        const grouped = groupBy(data, "age", "city");
        expect(grouped).toHaveLength(2);
    });
});

describe("formatDate", () => {
    const date = new Date("2024-01-15T10:30:45.000Z");

    it('returns "No date" for undefined', () => {
        expect(formatDate(undefined)).toBe("No date");
    });

    it("formats date with different time windows", () => {
        expect(formatDate(date)).toMatch(/\d{2}:\d{2}/);
        expect(formatDate(date, 3)).toMatch(/\d{2}:\d{2}:\d{2}-\d{2}/);
    });

    it("handles different durations", () => {
        const result = formatDate(date, 0, 7 * 24 * 60 * 60 * 1000);
        expect(result).toMatch(/Jan 15 \d{2}:\d{2}/);
    });
});

describe("findTicks", () => {
    it("finds ticks for different time ranges", () => {
        const start = new Date("2024-01-01");
        const end = new Date("2024-12-31");
        const ticks = findTicks(
            [{ x: new Date("2024-03-01") }, { x: new Date("2024-04-01") }],
            start,
            end
        );
        expect(ticks.length).toBeGreaterThan(0);
    });

    it("handles default parameters", () => {
        const ticks = findTicks([]);
        expect(Array.isArray(ticks)).toBe(true);
    });
});

describe("colorFor", () => {
    it("returns consistent color for same input", () => {
        const color1 = colorFor("test");
        const color2 = colorFor("test");
        expect(color1).toBe(color2);
    });

    it("returns different colors for different inputs", () => {
        const color1 = colorFor("test1");
        const color2 = colorFor("test2");
        expect(color1).not.toBe(color2);
    });

    it("supports opacity parameter", () => {
        const colorWithOpacity = colorFor("test", 0.5);
        expect(colorWithOpacity).toMatch(/\/ 0\.5\)/);
    });

    it("returns valid oklch format", () => {
        const color = colorFor("test");
        expect(color).toMatch(/oklch\(60% 80% \d+\)/);
    });
});
