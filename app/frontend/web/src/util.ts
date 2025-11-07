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
