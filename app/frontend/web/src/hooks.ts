import { useCallback, useEffect, useState } from "react";

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

const themeListeners: Set<(t: string) => void> = new Set();

function setupTheme(value: string) {
    if (value) {
        if (value === "system") {
            delete localStorage.theme;
        } else {
            localStorage.theme = value;
        }
    }
    for (const handle of themeListeners) {
        handle(value);
    }
    document.documentElement.classList.toggle(
        "dark",
        "theme" in localStorage
            ? localStorage.theme === "dark"
            : matchMedia("(prefers-color-scheme: dark)").matches
    );
}

export function useTheme(): [string, (v: string) => void] {
    const [selected, setSelected] = useState(localStorage.theme ?? "system");
    useEffect(() => {
        const handler = () => setSelected(localStorage.theme ?? "system");
        themeListeners.add(handler);
        addEventListener("storage", handler);
        return () => {
            removeEventListener("storage", handler);
            themeListeners.delete(handler);
        };
    }, []);
    return [selected, setupTheme];
}

function getQuery() {
    const hash = document.location.hash;
    const idx = hash.indexOf("?");
    if (idx >= 0) {
        return new URLSearchParams(hash.slice(idx + 1));
    } else {
        return new URLSearchParams();
    }
}

function getUrlWithQuery(q: URLSearchParams) {
    const href = document.location.href;
    const hashIdx = href.indexOf("#");
    if (hashIdx >= 0) {
        const idx = href.indexOf("?", hashIdx);
        if (idx >= 0) {
            return q.size === 0
                ? href.slice(0, idx)
                : href.slice(0, idx + 1) + q.toString();
        } else {
            return q.size === 0 ? href : href + "?" + q.toString();
        }
    } else {
        return q.size === 0 ? href : href + "#/?" + q.toString();
    }
}

function getParam<T>(name: string, def: T) {
    const query = getQuery();
    if (query.has(name)) {
        try {
            return JSON.parse(query.get(name)!) as T;
        } catch {
            // Do nothing, use the normal default.
        }
    }
    return def;
}

export function useParam<T>(name: string, def: T): [T, (v: T) => void] {
    let [value, setValue] = useState(getParam(name, def));
    useEffect(() => {
        setValue(getParam(name, def));
        const handler = () => setValue(getParam(name, def));
        addEventListener("hashchange", handler);
        return () => removeEventListener("hashchange", handler);
    }, [name]);
    const setInnerValue = useCallback(
        (v: T) => {
            const query = getQuery();
            if (JSON.stringify(def) !== JSON.stringify(v)) {
                query.set(name, JSON.stringify(v));
                setValue(v);
            } else {
                query.delete(name);
                setValue(def);
            }
            document.location.replace(getUrlWithQuery(query));
        },
        [name]
    );
    return [value, setInnerValue];
}
