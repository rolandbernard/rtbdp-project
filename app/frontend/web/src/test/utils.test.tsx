import { describe, it, expect } from "vitest";

import { EVENT_ICONS, boldQuery } from "../utils";
import { EVENT_KINDS } from "../api/tables";

describe("EVENT_ICONS", () => {
    it("contains all expected event types", () => {
        Object.keys(EVENT_KINDS).forEach(event => {
            expect(EVENT_ICONS).toHaveProperty(event);
        });
    });

    it("returns valid React components for all events", () => {
        Object.values(EVENT_ICONS).forEach(Icon => {
            expect(<Icon />).toBeDefined();
        });
    });
});

describe("boldQuery", () => {
    it("returns original text when query not found", () => {
        const result = boldQuery("Hello World", "xyz");
        expect(result).toMatchObject(
            <span key="query-highlight">Hello World</span>
        );
    });

    it("highlights matching text (case insensitive)", () => {
        const result = boldQuery("Hello World", "hello");
        expect(result).toMatchObject(
            <span key="query-highlight">
                <span></span>
                <span className="font-bold underline">Hello</span>
                <span> World</span>
            </span>
        );
    });

    it("highlights with custom style", () => {
        const result = boldQuery("Hello World", "world", "text-red-500");
        expect(result).toMatchObject(
            <span key="query-highlight">
                <span>Hello </span>
                <span className="text-red-500">World</span>
                <span></span>
            </span>
        );
    });

    it("handles empty query", () => {
        const result = boldQuery("Hello World", "");
        expect(result).toMatchObject(
            <span key="query-highlight">Hello World</span>
        );
    });

    it("handles empty text", () => {
        const result = boldQuery("", "test");
        expect(result).toMatchObject(<span key="query-highlight"></span>);
    });
});
