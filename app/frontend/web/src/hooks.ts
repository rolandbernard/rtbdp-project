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
