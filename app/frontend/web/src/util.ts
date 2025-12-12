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

function twoDigitString(number: number) {
    if (number < 10) {
        return "0" + number;
    } else {
        return number.toString();
    }
}

const MONTHS = [
    "Jan",
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

export function formatDate(
    date: Date | undefined,
    window: number = 0,
    dur = 24 * 60 * 60 * 1000
) {
    if (!date) {
        return "No date";
    }
    let result = "";
    if (dur > 365 * 24 * 60 * 60 * 1000) {
        result += date.getFullYear();
    }
    if (window >= 24 * 60 * 60 || dur > 24 * 60 * 60 * 1000) {
        result += " " + MONTHS[date.getMonth()] + " " + date.getDate();
    }
    if (window !== 0 || dur > 60 * 1000) {
        result +=
            " " +
            twoDigitString(date.getHours()) +
            ":" +
            twoDigitString(date.getMinutes());
    }
    if ((window !== 0 && window < 60) || dur < 5 * 60 * 1000) {
        result += ":" + twoDigitString(date.getSeconds());
    }
    if (window !== 0) {
        const endDate = new Date(date.getTime() + 1000 * window);
        result += "-";
        if (window >= 24 * 60 * 60) {
            result += MONTHS[endDate.getMonth()] + " " + endDate.getDate();
        }
        if (window >= 60 * 60) {
            if (result[result.length - 1] !== "-") {
                result += " ";
            }
            result += twoDigitString(endDate.getHours());
        }
        if (window >= 60) {
            if (result[result.length - 1] !== "-") {
                result += ":";
            }
            result += twoDigitString(endDate.getMinutes());
        }
        if (window < 60 || dur < 5 * 60 * 1000) {
            if (result[result.length - 1] !== "-") {
                result += ":";
            }
            result += twoDigitString(endDate.getSeconds());
        }
    }
    return result.trim();
}

function findTicksWithDiff(data: { x: Date }[], r: (d: Date) => number) {
    const ticks = [];
    let last = data[0]?.x;
    for (const d of data) {
        if (r(last!) !== r(d.x)) {
            ticks.push(d.x);
        }
        last = d.x;
    }
    return ticks;
}

export function findTicks(
    data: { x: Date }[],
    start: Date = new Date(),
    stop: Date = new Date()
) {
    const duration = stop.getTime() - start.getTime();
    if (duration > 5 * 365 * 24 * 60 * 60 * 1000) {
        return findTicksWithDiff(data, d => d.getFullYear());
    } else if (duration > 7 * 31 * 24 * 60 * 60 * 1000) {
        return findTicksWithDiff(data, d => d.getMonth());
    } else if (duration > 4 * 31 * 24 * 60 * 60 * 1000) {
        return findTicksWithDiff(
            data,
            d => Math.trunc(d.getDate() / 16) * 12 + d.getMonth()
        );
    } else if (duration > 31 * 24 * 60 * 60 * 1000) {
        return findTicksWithDiff(
            data,
            d => Math.trunc(d.getDate() / 4) * 12 + d.getMonth()
        );
    } else if (duration > 14 * 24 * 60 * 60 * 1000) {
        return findTicksWithDiff(
            data,
            d => Math.trunc(d.getDate() / 2) * 12 + d.getMonth()
        );
    } else if (duration > 7 * 24 * 60 * 60 * 1000) {
        return findTicksWithDiff(data, d => d.getDate());
    } else if (duration > 4 * 24 * 60 * 60 * 1000) {
        return findTicksWithDiff(
            data,
            d => Math.trunc(d.getHours() / 6) * 31 + d.getDate()
        );
    } else if (duration > 24 * 60 * 60 * 1000) {
        return findTicksWithDiff(
            data,
            d => Math.trunc(d.getHours() / 3) * 31 + d.getDate()
        );
    } else {
        return findTicksWithDiff(data, d => d.getHours());
    }
}

function hashString(str: string) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        hash += str.charCodeAt(i);
        hash = Math.imul(hash, 7919);
    }
    return ((hash % 359) + 359) % 359;
}

export function colorFor(val: string, opacity?: number) {
    const hue = hashString(val);
    if (opacity) {
        return `oklch(60% 80% ${hue} / ${opacity})`;
    } else {
        return `oklch(60% 80% ${hue})`;
    }
}
