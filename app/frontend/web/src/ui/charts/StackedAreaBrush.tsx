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

import { colorFor, findTicks, formatDate } from "../../util";

import TimeTooltip from "./TimeTooltip";
import { computeFactor, VarBrush } from "./VarBrush";

type DataRow = { x: Date; y: number; [k: string]: number | Date };

interface Props {
    keys: string[];
    data: DataRow[];
    chartColor: string;
    window: number;
    highligh?: string;
    onClick?: (v: string) => void;
    onRangeChange?: (range: number) => void;
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
    const newFactor = computeFactor(props.data.length, startIdx, endIdx);
    let [factor, setFactor] = useState(newFactor);
    if (newFactor !== factor && endIdx == null) {
        factor = newFactor;
        setFactor(newFactor);
    }
    const cleanData = useMemo(() => {
        if (factor > 1) {
            const reduced = [];
            for (let i = props.data.length - 1; i >= 0; i -= factor) {
                const row = {
                    x: props.data[Math.max(0, i + 1 - factor)]!.x,
                } as DataRow;
                for (const key of [...props.keys, "y"]) {
                    let sum = 0;
                    for (let j = 0; j < factor && i - j >= 0; j++) {
                        sum += props.data[i - j]![key]! as number;
                    }
                    row[key] = sum;
                }
                reduced.push(row);
            }
            return reduced.reverse();
        } else {
            return props.data;
        }
    }, [props.keys, props.data, factor]);
    const formatTick = (d: Date) => formatDate(d, 0, min_dur, min_dur);
    const formatTick2 = (d: Date) => formatDate(d, 0, min_dur, max_dur);
    return (
        <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={cleanData}>
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
