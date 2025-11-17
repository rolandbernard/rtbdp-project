import { useCallback, useEffect, useMemo, useState } from "react";
import { useLocation } from "react-router";

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

type ParamType = string[] | number[] | string | number;

function encodeParam<T extends ParamType>(obj: T) {
    if (obj instanceof Array) {
        return "[" + obj.map(e => e.toString()).join(",") + "]";
    } else {
        return obj.toString();
    }
}

function isNumber(str: string) {
    for (let i = 0; i < str.length; i++) {
        if (str.codePointAt(i)! < 48 || str.codePointAt(i)! > 57) {
            return false;
        }
    }
    return true;
}

function decodeParam<T extends ParamType>(val: string) {
    if (val[0] === "[") {
        const array = val
            .substring(1, val.length - 1)
            .split(",")
            .filter(e => e.length !== 0)
            .map(e => (isNumber(e) ? parseInt(e) : e));
        return array as T;
    } else {
        return (isNumber(val) ? parseInt(val) : val) as T;
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

function getCurrentQuery() {
    const href = document.location.href;
    const hashIdx = href.indexOf("#");
    if (hashIdx >= 0) {
        const idx = href.indexOf("?", hashIdx);
        if (idx >= 0) {
            return new URLSearchParams(href.slice(idx));
        } else {
            return new URLSearchParams();
        }
    } else {
        return new URLSearchParams();
    }
}

export function useParam<T extends ParamType>(
    name: string,
    def: T
): [T, (v: T) => void] {
    const location = useLocation();
    const defValue = useMemo(() => encodeParam(def), [def]);
    const [value, setInnerValue] = useState(() =>
        decodeParam<T>(getCurrentQuery().get(name) ?? defValue)
    );
    useEffect(() => {
        setInnerValue(decodeParam<T>(getCurrentQuery().get(name) ?? defValue));
    }, [location.search, defValue, name]);
    const setValue = useCallback(
        (v: T) => {
            let newValue: string | null = encodeParam(v);
            if (newValue == defValue) {
                newValue = null;
            }
            const query = getCurrentQuery();
            if (query.get(name) != newValue) {
                setInnerValue(v);
                if (newValue) {
                    query.set(name, newValue);
                } else {
                    query.delete(name);
                }
                document.location.replace(getUrlWithQuery(query));
            }
        },
        [name, defValue]
    );
    return [value, setValue];
}
