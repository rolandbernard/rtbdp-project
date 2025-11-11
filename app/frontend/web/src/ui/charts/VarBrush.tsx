import { AreaChart, Area, Brush } from "recharts";
import type { BrushStartEndIndex } from "recharts/types/context/brushUpdateContext";

export function computeFactor(len: number, startIdx: number, endIdx?: number) {
    return Math.max(1, Math.ceil(((endIdx ?? len - 1) - startIdx + 1) / 100));
}

interface Props {
    len: number;
    startIdx: number;
    endIdx?: number;
    factor: number;
    chartColor: string;
    formatTicks: (d: Date) => string;
    setStart: (v: number) => void;
    setEnd: (v: number) => void;
    setFactor: (v: number) => void;
}

export function VarBrush(props: Props) {
    const setStartEnd = (e: BrushStartEndIndex) => {
        const start = Math.min(e.startIndex * props.factor, props.len - 1);
        const end = Math.min(
            e.endIndex * props.factor + props.factor - 1,
            props.len - 1
        );
        props.setStart(start);
        props.setEnd(end);
        return [start, end] as [number, number];
    };
    return (
        <Brush
            dataKey="x"
            tickFormatter={props.formatTicks}
            fill="var(--color-base-100)"
            stroke="var(--color-border)"
            startIndex={Math.trunc(props.startIdx / props.factor)}
            endIndex={
                props.endIdx
                    ? Math.trunc(props.endIdx / props.factor)
                    : undefined
            }
            onChange={setStartEnd}
            onDragEnd={e => {
                const [start, end] = setStartEnd(e);
                props.setFactor(computeFactor(props.len, start, end));
            }}
        >
            <AreaChart>
                <defs>
                    <linearGradient
                        id={"colorGradient" + btoa(props.chartColor)}
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
                <Area
                    type="monotone"
                    dataKey="y"
                    stroke={props.chartColor}
                    fill={`url(#colorGradient${btoa(props.chartColor)})`}
                    animationDuration={200}
                    animationEasing="linear"
                />
            </AreaChart>
        </Brush>
    );
}
