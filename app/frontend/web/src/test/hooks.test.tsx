import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { renderHook, act } from "@testing-library/react";

import { useDebounce, useLatched } from "../hooks";

describe("useDebounce", () => {
    beforeEach(() => {
        vi.useFakeTimers();
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    it("returns immediate value when millis is 0", () => {
        const { result, rerender } = renderHook(
            ({ value, delay }) => useDebounce(value, delay),
            { initialProps: { value: "initial", delay: 0 } }
        );
        expect(result.current).toBe("initial");
        rerender({ value: "updated", delay: 0 });
        expect(result.current).toBe("updated");
    });

    it("delays value update when millis > 0", () => {
        const { result, rerender } = renderHook(
            ({ value, delay }) => useDebounce(value, delay),
            { initialProps: { value: "initial", delay: 500 } }
        );
        expect(result.current).toBe("initial");
        rerender({ value: "updated", delay: 500 });
        expect(result.current).toBe("initial");
        act(() => {
            vi.advanceTimersByTime(500);
        });
        expect(result.current).toBe("updated");
    });

    it("clears previous timeout on value change", () => {
        const { result, rerender } = renderHook(
            ({ value, delay }) => useDebounce(value, delay),
            { initialProps: { value: "initial", delay: 500 } }
        );
        rerender({ value: "updated1", delay: 500 });
        rerender({ value: "updated2", delay: 500 });
        act(() => {
            vi.advanceTimersByTime(500);
        });
        expect(result.current).toBe("updated2");
    });
});

describe("useLatched", () => {
    it("returns initial value when latch is false", () => {
        const { result, rerender } = renderHook(
            ({ value, latch }) => useLatched(value, latch),
            { initialProps: { value: "initial", latch: false } }
        );
        expect(result.current).toBe("initial");
        rerender({ value: "updated", latch: false });
        expect(result.current).toBe("initial");
    });

    it("returns current value when latch is true", () => {
        const { result, rerender } = renderHook(
            ({ value, latch }) => useLatched(value, latch),
            { initialProps: { value: "initial", latch: true } }
        );
        expect(result.current).toBe("initial");
        rerender({ value: "updated", latch: true });
        expect(result.current).toBe("updated");
    });

    it("latches when latch becomes true", () => {
        const { result, rerender } = renderHook(
            ({ value, latch }) => useLatched(value, latch),
            { initialProps: { value: "initial", latch: false } }
        );
        expect(result.current).toBe("initial");
        rerender({ value: "updated1", latch: true });
        expect(result.current).toBe("updated1");
        rerender({ value: "updated2", latch: true });
        expect(result.current).toBe("updated2");
        rerender({ value: "updated3", latch: false });
        expect(result.current).toBe("updated2");
    });
});
