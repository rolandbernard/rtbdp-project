import { useState } from "react";
import {
    AreaChart,
    ResponsiveContainer,
    XAxis,
    Tooltip,
    YAxis,
    Brush,
    Area,
    CartesianGrid,
} from "recharts";
import type { AxisTick } from "recharts/types/util/types";

import TimeTooltip from "./TimeTooltip";
import { formatDate } from "../../util";

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

function findTicks(start: Date = new Date(), stop: Date = new Date()) {
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

interface Props {
    data: { x: Date; y: number }[];
    chartColor: string;
    window?: number;
}

export default function AreaBrush(props: Props) {
    const [startIdx, setStart] = useState(0);
    const [endIdx, setEnd] = useState<number | undefined>(undefined);
    const start = props.data[startIdx]?.x;
    const stop = props.data[endIdx ?? props.data.length - 1]?.x;
    const max_dur =
        props.data.length > 0
            ? props.data[props.data.length - 1]!.x.getTime() -
              props.data[0]!.x.getTime()
            : undefined;
    const min_dur =
        start && stop ? stop.getTime() - start.getTime() : undefined;
    const formatTick = (d: Date) => formatDate(d, 0, min_dur, min_dur);
    const formatTick2 = (d: Date) => formatDate(d, 0, min_dur, max_dur);
    return (
        <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={props.data}>
                <defs>
                    <linearGradient
                        id="colorGradient"
                        x1="0"
                        y1="0"
                        x2="0"
                        y2="1"
                    >
                        <stop
                            offset="10%"
                            stopColor={props.chartColor}
                            stopOpacity={0.5}
                        />
                        <stop
                            offset="100%"
                            stopColor={props.chartColor}
                            stopOpacity={0.1}
                        />
                    </linearGradient>
                    <linearGradient
                        id="colorGradient2"
                        x1="0"
                        y1="0"
                        x2="0"
                        y2="1"
                    >
                        <stop
                            offset="40%"
                            stopColor={props.chartColor}
                            stopOpacity={0.7}
                        />
                        <stop
                            offset="100%"
                            stopColor={props.chartColor}
                            stopOpacity={0.5}
                        />
                    </linearGradient>
                </defs>
                <CartesianGrid
                    strokeDasharray="3 3"
                    className="stroke-border"
                />
                <XAxis
                    dataKey="x"
                    tickFormatter={formatTick}
                    ticks={findTicks(start, stop) as unknown as AxisTick[]}
                />
                <YAxis />
                <Tooltip
                    content={
                        <TimeTooltip
                            window={props.window}
                            start={start}
                            stop={stop}
                        />
                    }
                />
                <Brush
                    dataKey="x"
                    tickFormatter={formatTick2}
                    fill="var(--color-base-100)"
                    stroke="var(--color-border)"
                    startIndex={startIdx}
                    endIndex={endIdx}
                    onChange={e => {
                        setStart(e.startIndex);
                        setEnd(e.endIndex);
                    }}
                >
                    <AreaChart>
                        <Area
                            type="monotone"
                            dataKey="y"
                            stroke={props.chartColor}
                            fill="url(#colorGradient)"
                            animationDuration={200}
                            animationEasing="linear"
                        />
                    </AreaChart>
                </Brush>
                <Area
                    type="monotone"
                    dataKey="y"
                    stroke={props.chartColor}
                    strokeWidth={2}
                    fill="url(#colorGradient2)"
                    animationDuration={200}
                    animationEasing="linear"
                />
            </AreaChart>
        </ResponsiveContainer>
    );
}
