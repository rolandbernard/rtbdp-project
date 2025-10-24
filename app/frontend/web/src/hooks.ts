import { useEffect, useState } from "react";

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

export function useLatched<T>(value: T, latch: boolean) {
    const [latched, setLatched] = useState(value);
    useEffect(() => {
        if (latch) {
            setLatched(value);
        }
    }, [latch, value]);
    return latch ? value : latched;
}
