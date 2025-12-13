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

import { colorFor, findTicks, formatDate } from "../../util";

import TimeTooltip from "./TimeTooltip";
import { computeFactor, filterData, VarBrush } from "./VarBrush";

type DataRow = { x: Date; y: number; [k: string]: number | Date };

interface Props {
    keys: string[];
    data: DataRow[];
    chartColor: string;
    className?: string;
    window: number;
    highligh?: string;
    onClick?: (v: string) => void;
    onRangeChange?: (range: number) => void;
}

export default function StackedAreaBrush(props: Props) {
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
        const factor = computeFactor(props.data, start, stop, 100);
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
                const row = {
                    x: filtered[Math.max(0, i + 1 - factor)]!.x,
                } as DataRow;
                for (const key of [...props.keys, "y"]) {
                    let sum = 0;
                    for (let j = 0; j < factor && i - j >= 0; j++) {
                        sum += filtered[i - j]![key]! as number;
                    }
                    row[key] = sum;
                }
                reduced.push(row);
            }
            return [factor, reduced.reverse()];
        } else {
            return [factor, filtered];
        }
    }, [props.keys, props.data, props.window, start, stop]);
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
                <ResponsiveContainer
                    width="100%"
                    height="100%"
                    className="contain-strict"
                >
                    <AreaChart data={cleanData} syncId="syncIdStacked">
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
                        {props.keys.sort().map(key => {
                            const opacity =
                                props.highligh && props.highligh !== key
                                    ? 0.4
                                    : 1.0;
                            return (
                                <Area
                                    key={key}
                                    type="monotone"
                                    dataKey={key}
                                    fill={colorFor(key, opacity)}
                                    stroke={colorFor(key, opacity)}
                                    strokeWidth={opacity == 0.4 ? 1 : 2}
                                    stackId="1"
                                    onClick={() => props.onClick?.(key)}
                                    className="cursor-pointer"
                                    isAnimationActive={false}
                                />
                            );
                        })}
                    </AreaChart>
                </ResponsiveContainer>
            </div>
            <VarBrush
                name="Stacked"
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
