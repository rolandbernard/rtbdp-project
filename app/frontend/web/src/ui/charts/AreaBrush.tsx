import { useMemo, useState } from "react";
import type { AxisTick } from "recharts/types/util/types";
import {
    AreaChart,
    ResponsiveContainer,
    XAxis,
    Tooltip,
    YAxis,
    Area,
    CartesianGrid,
} from "recharts";

import { findTicks, formatDate } from "../../util";

import TimeTooltip from "./TimeTooltip";
import { computeFactor, filterData, VarBrush } from "./VarBrush";

interface Props {
    name: string;
    data: { x: Date; y: number }[];
    chartColor: string;
    window: number;
    className?: string;
}

export default function AreaBrush(props: Props) {
    const [rawStart, setStart] = useState<Date | undefined>(undefined);
    const [rawStop, setStop] = useState<Date | undefined>(undefined);
    const stop = useMemo(
        () => rawStop ?? props.data[props.data.length - 1]?.x ?? new Date(),
        [rawStop, props.data]
    );
    const start = useMemo(() => {
        if (rawStart) {
            return rawStart;
        } else {
            const startA = new Date(stop.getTime() - 24 * 60 * 60 * 1000);
            const startB = props.data[0]?.x ?? new Date();
            return startA > startB ? startA : startB;
        }
    }, [rawStart, stop, props.data]);
    const [factor, cleanData] = useMemo(() => {
        const factor = computeFactor(props.data, start, stop);
        const align = factor * props.window * 1000;
        const extStart = new Date(align * Math.floor(start.getTime() / align));
        const extStop = new Date(
            align * Math.ceil(stop.getTime() / align) - props.window * 1000
        );
        const filtered = filterData(
            props.data,
            extStart,
            extStop >= extStart ? extStop : extStart
        );
        if (factor > 1) {
            const reduced = [];
            for (let i = filtered.length - 1; i >= 0; i -= factor) {
                let sum = 0;
                for (let j = 0; j < factor && i - j >= 0; j++) {
                    sum += filtered[i - j]!.y;
                }
                reduced.push({
                    x: filtered[Math.max(0, i + 1 - factor)]!.x,
                    y: sum,
                });
            }
            return [factor, reduced.reverse()];
        } else {
            return [factor, filtered];
        }
    }, [props.data, props.window, start, stop]);
    const ticks = useMemo(
        () => findTicks(cleanData, start, stop),
        [cleanData, start, stop]
    );
    const formatTick = (d: Date) =>
        formatDate(d, 0, stop.getTime() - start.getTime());
    return (
        <div
            className={"w-full h-full flex flex-col " + (props.className ?? "")}
        >
            <div className="grow min-h-0 contain-strict select-none">
                <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={cleanData} syncId={"syncId" + props.name}>
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
                                    stopOpacity={0.8}
                                />
                                <stop
                                    offset="100%"
                                    stopColor={props.chartColor}
                                    stopOpacity={0.6}
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
                            ticks={ticks as unknown as AxisTick[]}
                            minTickGap={20}
                        />
                        <YAxis />
                        <Tooltip
                            content={
                                <TimeTooltip
                                    window={props.window * factor}
                                    start={props.data[0]?.x}
                                    stop={props.data[props.data.length - 1]?.x}
                                />
                            }
                        />
                        <Area
                            type="monotone"
                            dataKey="y"
                            stroke={props.chartColor}
                            strokeWidth={2}
                            fill={`url(#colorGradient2${btoa(
                                props.chartColor
                            )})`}
                            isAnimationActive={false}
                        />
                    </AreaChart>
                </ResponsiveContainer>
            </div>
            <VarBrush
                name={props.name}
                data={props.data}
                window={props.window}
                start={start}
                stop={stop}
                chartColor={props.chartColor}
                setStartStop={(start, stop) => {
                    setStart(start);
                    setStop(
                        stop?.getTime() ==
                            props.data[props.data.length - 1]?.x.getTime()
                            ? undefined
                            : stop
                    );
                }}
            />
        </div>
    );
}
