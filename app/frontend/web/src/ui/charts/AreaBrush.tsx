import { useMemo, useState } from "react";
import {
    AreaChart,
    ResponsiveContainer,
    XAxis,
    Tooltip,
    YAxis,
    Area,
    CartesianGrid,
} from "recharts";
import type { AxisTick } from "recharts/types/util/types";

import { findTicks, formatDate } from "../../util";

import TimeTooltip from "./TimeTooltip";
import { computeFactor, VarBrush } from "./VarBrush";

interface Props {
    data: { x: Date; y: number }[];
    chartColor: string;
    window: number;
}

export default function AreaBrush(props: Props) {
    const [startIdx, setStart] = useState<number | undefined>(undefined);
    const [endIdx, setEnd] = useState<number | undefined>(undefined);
    const start =
        props.data[startIdx ?? Math.max(0, props.data.length - 288)]?.x;
    const stop = props.data[endIdx ?? props.data.length - 1]?.x;
    const max_dur =
        props.data.length > 0
            ? props.data[props.data.length - 1]!.x.getTime() -
              props.data[0]!.x.getTime()
            : undefined;
    const min_dur =
        start && stop ? stop.getTime() - start.getTime() : undefined;
    const newFactor = computeFactor(props.data.length, startIdx, endIdx);
    const [f, setFactor] = useState(newFactor);
    let factor = f;
    if (newFactor !== factor && endIdx == null) {
        factor = newFactor;
        setFactor(newFactor);
    }
    const cleanData = useMemo(() => {
        if (factor > 1) {
            const reduced = [];
            for (let i = props.data.length - 1; i >= 0; i -= factor) {
                let sum = 0;
                for (let j = 0; j < factor && i - j >= 0; j++) {
                    sum += props.data[i - j]!.y;
                }
                reduced.push({
                    x: props.data[Math.max(0, i + 1 - factor)]!.x,
                    y: sum,
                });
            }
            return reduced.reverse();
        } else {
            return props.data;
        }
    }, [props.data, factor]);
    const formatTick = (d: Date) => formatDate(d, 0, min_dur, min_dur);
    const formatTick2 = (d: Date) => formatDate(d, 0, min_dur, max_dur);
    return (
        <ResponsiveContainer
            width="100%"
            height="100%"
            className="contain-strict"
        >
            <AreaChart data={cleanData}>
                <defs>
                    <linearGradient
                        id={"colorGradient2" + btoa(props.chartColor)}
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
                            window={props.window * factor}
                            start={start}
                            stop={stop}
                            small={false}
                        />
                    }
                />
                <VarBrush
                    len={props.data.length}
                    startIdx={startIdx}
                    endIdx={endIdx}
                    factor={factor}
                    chartColor={props.chartColor}
                    formatTicks={formatTick2}
                    setStart={setStart}
                    setEnd={setEnd}
                    setFactor={setFactor}
                />
                <Area
                    type="monotone"
                    dataKey="y"
                    stroke={props.chartColor}
                    strokeWidth={2}
                    fill={`url(#colorGradient2${btoa(props.chartColor)})`}
                    isAnimationActive={false}
                />
            </AreaChart>
        </ResponsiveContainer>
    );
}
