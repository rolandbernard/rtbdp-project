import { useEffect, useState } from "react";

export function useDebounce<T>(value: T, millis: number) {
    const [debounced, setDebounced] = useState(value);
    useEffect(() => {
        if (millis !== 0) {
            const timeout = setTimeout(() => {
                setDebounced(value);
            }, millis);
            return () => clearTimeout(timeout);
        }
    }, [value, millis]);
    return millis === 0 ? value : debounced;
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

export function useClickOutside(target: string, func: () => void) {
    useEffect(() => {
        const handler = (e: MouseEvent) => {
            if (!(e.target as HTMLElement)?.closest(target)) {
                func();
            }
        };
        document.addEventListener("click", handler);
        return () => document.removeEventListener("click", handler);
    }, [target, func]);
}
