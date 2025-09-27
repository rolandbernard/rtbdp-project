import { useEffect, useState } from "react";

export function sortedKey<T, C>(
    fn: ((a: T) => C)[],
    rev = false
): (a: T, b: T) => number {
    if (fn.length == 1) {
        const f = fn[0]!;
        return (a, b) => {
            const [fa, fb] = [f(a), f(b)];
            const cmp = fa < fb ? -1 : fa > fb ? 1 : 0;
            return rev ? -cmp : cmp;
        };
    } else {
        const rest = sortedKey(fn.splice(1), rev);
        const f = fn[0]!;
        return (a, b) => {
            const [fa, fb] = [f(a), f(b)];
            const cmp = fa < fb ? -1 : fa > fb ? 1 : 0;
            if (cmp != 0) {
                return rev ? -cmp : cmp;
            } else {
                return rest(a, b);
            }
        };
    }
}

export function sort<T, C>(
    array: T[],
    fn: ((a: T) => C)[],
    rev?: boolean
): T[] {
    return array.toSorted(sortedKey<T, C>(fn, rev));
}

export function groupKey<R>(row: R, keys: (keyof R)[]): string {
    return keys.map(k => row[k]).join(":");
}

export function groupBy<R>(table: R[], ...keys: (keyof R)[]): R[][] {
    return Object.values(
        Object.groupBy(table, row => groupKey(row, keys))
    ) as R[][];
}

export function useDebounce<T>(value: T, millis: number) {
    const [debounced, setDebounced] = useState(value);
    useEffect(() => {
        const timeout = setTimeout(() => {
            setDebounced(value);
        }, millis);
        return () => clearTimeout(timeout);
    }, [value, millis]);
    return debounced;
}
