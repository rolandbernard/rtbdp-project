type Comparable = number | string | Date;

export function sortedKey<T>(
    fn: ((a: T) => Comparable)[],
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

export function sort<T>(
    array: T[],
    fn: ((a: T) => Comparable)[],
    rev?: boolean
): T[] {
    return array.toSorted(sortedKey<T>(fn, rev));
}

export function groupKey<R>(row: R, keys: (keyof R)[]): string {
    return keys.map(k => row[k]).join(":");
}

export function groupBy<R>(table: R[], ...keys: (keyof R)[]): R[][] {
    return Object.values(
        Object.groupBy(table, row => groupKey(row, keys))
    ) as R[][];
}

export function asArray<T>(
    x: string | null,
    map: (e: string) => T,
    def: T[] = []
): T[] {
    if (x) {
        return x.split(",").map(map);
    } else {
        return def;
    }
}

export function asValue(x: (string | number)[]): string {
    return x.map(r => r.toString()).join(",");
}

function twoDigitString(number: number) {
    if (number < 10) {
        return "0" + number;
    } else {
        return number.toString();
    }
}

const MONTHS = [
    "Jab",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
];

export function formatDate(date: Date, window: number = 0, min_dur = 24 * 60 * 60 * 1000, max_dur = 24 * 60 * 60 * 1000) {
    let result = "";
    if (max_dur > 365 * 24 * 60 * 60 * 1000) {
        result += date.getFullYear();
    } 
    if (max_dur > 30 * 24 * 60 * 60 * 1000 && min_dur < 5 * 365 * 24 * 60 * 60 * 1000) {
        result += " " + MONTHS[date.getMonth()];
    } 
    if (max_dur > 24 * 60 * 60 * 1000 && min_dur < 7 * 31 * 24 * 60 * 60 * 1000) {
        result += " " + date.getDate();
    }
    if (window !== 0 || (max_dur > 60 * 60 * 1000 && min_dur < 7 * 24 * 60 * 60 * 1000)) {
        result += " " + twoDigitString(date.getHours());
    }
    if (window !== 0 || (max_dur > 60 * 1000 && min_dur < 5 * 60 * 60 * 1000)) {
        result += ":" + twoDigitString(date.getMinutes());
    }
    if ((window !== 0 && window < 60) || min_dur < 5 * 60 * 1000) {
        result += ":" + twoDigitString(date.getSeconds());
    }
    if (window !== 0) {
        result += "-";
        if (window >= 60) {
            result += twoDigitString((new Date(date.getTime() + 1000 * window)).getMinutes());
        }
        if (window < 60 || min_dur < 5 * 60 * 1000) {
            if (result[result.length - 1] !== "-") {
                result += ":";
            }
            result += twoDigitString((new Date(date.getTime() + 1000 * window)).getSeconds());
        }
    }
    return result.trim();
}

function findTicksWithDiff(start: Date, stop: Date, r: (d: Date) => number) {
    const ticks = [];
    const diff = 300_000;
    let curr = start;
    while (curr < stop) {
        const next = new Date(curr.getTime() + diff);
        if (r(curr) !== r(next)) {
            ticks.push(next);
        }
        curr = next;
    }
    return ticks;
}

export function findTicks(start: Date = new Date(), stop: Date = new Date()) {
    const duration = stop.getTime() - start.getTime();
    if (duration > 5 * 365 * 24 * 60 * 60 * 1000) {
        return findTicksWithDiff(start, stop, d => d.getFullYear());
    } else if (duration > 7 * 31 * 24 * 60 * 60 * 1000) {
        return findTicksWithDiff(start, stop, d => d.getMonth());
    } else if (duration > 4 * 31 * 24 * 60 * 60 * 1000) {
        return findTicksWithDiff(
            start,
            stop,
            d => Math.round(d.getDate() / 16) * 12 + d.getMonth()
        );
    } else if (duration > 31 * 24 * 60 * 60 * 1000) {
        return findTicksWithDiff(
            start,
            stop,
            d => Math.round(d.getDate() / 4) * 12 + d.getMonth()
        );
    } else if (duration > 14 * 24 * 60 * 60 * 1000) {
        return findTicksWithDiff(
            start,
            stop,
            d => Math.round(d.getDate() / 2) * 12 + d.getMonth()
        );
    } else if (duration > 7 * 24 * 60 * 60 * 1000) {
        return findTicksWithDiff(start, stop, d => d.getDate());
    } else if (duration > 4 * 24 * 60 * 60 * 1000) {
        return findTicksWithDiff(
            start,
            stop,
            d => Math.round(d.getHours() / 6) * 31 + d.getDate()
        );
    } else if (duration > 24 * 60 * 60 * 1000) {
        return findTicksWithDiff(
            start,
            stop,
            d => Math.round(d.getHours() / 3) * 31 + d.getDate()
        );
    } else {
        return findTicksWithDiff(start, stop, d => d.getHours());
    }
}

function hashString(str: string) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      hash += str.charCodeAt(i);
      hash = Math.imul(hash, 7919);
    }
    return hash % 359;
}

export function colorFor(val: string, opacity?: string) {
    const hue = hashString(val);
    if (opacity) {
        return `oklch(60% 80% ${hue} / ${opacity})`;
    } else {
        return `oklch(60% 80% ${hue})`;
    }
}
