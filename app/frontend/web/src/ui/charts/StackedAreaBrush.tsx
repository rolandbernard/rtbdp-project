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
import { colorFor, findTicks, formatDate } from "../../util";

interface Props {
    keys: string[];
    data: { x: Date; y: number; [k: string]: number | Date }[];
    chartColor: string;
    window?: number;
    highligh?: string;
    onClick?: (v: string) => void;
}

export default function StackedAreaBrush(props: Props) {
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
                {props.keys.sort().map(key => {
                    const opacity =
                        props.highligh && props.highligh !== key
                            ? "0.4"
                            : "1.0";
                    return (
                        <Area
                            key={key}
                            type="monotone"
                            dataKey={key}
                            fill={colorFor(key, opacity)}
                            stroke={colorFor(key, opacity)}
                            strokeWidth={opacity == "0.4" ? 1 : 2}
                            animationDuration={200}
                            animationEasing="linear"
                            stackId="1"
                            onClick={() => props.onClick?.(key)}
                            className="cursor-pointer"
                        />
                    );
                })}
            </AreaChart>
        </ResponsiveContainer>
    );
}
