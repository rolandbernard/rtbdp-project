import { useEffect, useMemo, useRef } from "react";
import { AreaChart, Area, ResponsiveContainer, XAxis, Tooltip } from "recharts";
import type { TickItem } from "recharts/types/util/types";
import type { MouseHandlerDataParam } from "recharts/types/synchronisation/types";
import { GripVertical } from "lucide-react";
import { formatDate } from "../../util";

export function filterData<T extends { x: Date }>(
    data: T[],
    start?: Date,
    stop?: Date
) {
    return data.filter(r => {
        if (start && r.x < start) {
            return false;
        }
        if (stop && r.x > stop) {
            return false;
        }
        return true;
    });
}

function computeIndex(data: { x: Date }[], start?: Date, stop?: Date) {
    const startIdx = data.findIndex(r => (start ? r.x >= start : true));
    const stopIdx = data.findLastIndex(r => (stop ? r.x <= stop : true));
    return [startIdx, stopIdx] as [number, number];
}

export function computeFactor(
    data: { x: Date }[],
    start?: Date,
    stop?: Date,
    lim = 150
) {
    const [startIdx, stopIdx] = computeIndex(data, start, stop);
    return Math.max(1, Math.ceil((stopIdx - startIdx + 1) / lim));
}

function syncIndex(
    ticks: ReadonlyArray<TickItem>,
    data: MouseHandlerDataParam
) {
    const date = data.activeLabel as unknown as Date;
    const array = ticks as unknown as { value: Date }[];
    if (
        array.length === 0 ||
        array[0]!.value > date ||
        array[array.length - 1]!.value < date
    ) {
        return -1;
    } else {
        const index = array.findIndex(r => r.value >= date);
        return index;
    }
}

function NoTooltip() {
    return <></>;
}

interface Props {
    name: string;
    data: { x: Date; y: number }[];
    chartColor: string;
    window: number;
    start: Date;
    stop: Date;
    setStartStop: (start?: Date, stop?: Date) => void;
}

export function VarBrush(props: Props) {
    const cleanData = useMemo(() => {
        const factor = computeFactor(props.data, undefined, undefined, 200);
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
    }, [props.data]);
    const container = useRef<HTMLDivElement>(null);
    const dragger = useRef<HTMLDivElement>(null);
    useEffect(() => {
        const elem = container.current!;
        const updateSize = () => {
            const full = elem.clientWidth - 24;
            const startIdx = props.data.findIndex(r =>
                props.start ? r.x >= props.start : true
            );
            const stopIdx = props.data.findLastIndex(r =>
                props.stop ? r.x <= props.stop : true
            );
            dragger.current!.style.left = `${
                (full * startIdx) / props.data.length
            }px`;
            dragger.current!.style.width = `${
                24 + (full * (stopIdx - startIdx + 1)) / props.data.length
            }px`;
        };
        const observer = new ResizeObserver(updateSize);
        observer.observe(elem);
        updateSize();
        return () => observer.unobserve(elem);
    });
    const onDown = (e: { clientX: number }, f: string) => {
        const start = e.clientX;
        const onMove = (e: { clientX: number }) => {
            const elem = container.current!.getBoundingClientRect();
            if (f === "move") {
                const moved = e.clientX - start;
                const delta = Math.round(
                    (moved * props.data.length) / (elem.width - 24)
                );
                let [startIdx, stopIdx] = computeIndex(
                    props.data,
                    props.start,
                    props.stop
                );
                startIdx = Math.min(
                    Math.max(startIdx + delta, 0),
                    props.data.length - 1
                );
                stopIdx = Math.min(
                    Math.max(stopIdx + delta, 0),
                    props.data.length - 1
                );
                props.setStartStop(
                    props.data[startIdx]!.x,
                    props.data[stopIdx]!.x
                );
            } else {
                const pos = e.clientX - (f === "start" ? 6 : 20) - elem.left;
                const index = Math.min(
                    Math.max(
                        Math.round(
                            (pos * props.data.length) / (elem.width - 24)
                        ),
                        0
                    ),
                    props.data.length - 1
                );
                const value = props.data[index]!.x;
                if (f === "start") {
                    props.setStartStop(
                        value < props.stop ? value : props.stop,
                        props.stop
                    );
                } else {
                    props.setStartStop(
                        props.start,
                        value > props.start ? value : props.start
                    );
                }
            }
        };
        const onTouch = (e: TouchEvent) => {
            onMove(e.touches[0]!);
        };
        const onUp = () => {
            removeEventListener("mousemove", onMove);
            removeEventListener("mouseup", onUp);
            removeEventListener("touchmove", onTouch);
            removeEventListener("touchend", onUp);
            removeEventListener("touchcancel", onUp);
            removeEventListener("mouseleave", onUp);
            removeEventListener("blur", onUp);
        };
        addEventListener("mousemove", onMove);
        addEventListener("mouseup", onUp);
        addEventListener("touchmove", onTouch);
        addEventListener("touchend", onUp);
        addEventListener("touchcancel", onUp);
        addEventListener("mouseleave", onUp);
        addEventListener("blur", onUp);
    };
    return (
        <div className="pl-10">
            <div
                className="contain-strict h-10 relative pr-3 pl-3"
                ref={container}
            >
                <ResponsiveContainer
                    width="100%"
                    height="100%"
                    className="pointer-events-none"
                >
                    <AreaChart
                        data={cleanData}
                        syncId={"syncId" + props.name}
                        syncMethod={syncIndex}
                        margin={{ bottom: 0, left: 0, top: 0, right: 0 }}
                    >
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
                        <XAxis dataKey="x" hide={true} />
                        <Tooltip content={<NoTooltip />} />
                        <Area
                            type="monotone"
                            dataKey="y"
                            stroke={props.chartColor}
                            fill={`url(#colorGradient${btoa(
                                props.chartColor
                            )})`}
                            isAnimationActive={false}
                        />
                    </AreaChart>
                </ResponsiveContainer>
                <div
                    className={
                        "absolute top-0 h-full flex flex-row " +
                        (props.data.length <= 1 ? "hidden" : "")
                    }
                    ref={dragger}
                    title={
                        formatDate(props.start, 0, Infinity) +
                        " to " +
                        formatDate(props.stop, 0, Infinity)
                    }
                >
                    <div
                        className="h-full w-3 bg-content/50 cursor-col-resize flex items-center justify-center rounded-l-md"
                        onMouseDown={e => onDown(e, "start")}
                        onTouchStart={e => onDown(e.touches[0]!, "start")}
                    >
                        <GripVertical className="text-base-100" />
                    </div>
                    <div
                        className="w-full h-full bg-content/20 cursor-grab"
                        onMouseDown={e => onDown(e, "move")}
                        onTouchStart={e => onDown(e.touches[0]!, "move")}
                    />
                    <div
                        className="h-full w-3 bg-content/50 cursor-col-resize flex items-center justify-center rounded-r-md"
                        onMouseDown={e => onDown(e, "stop")}
                        onTouchStart={e => onDown(e.touches[0]!, "stop")}
                    >
                        <GripVertical className="text-base-100" />
                    </div>
                </div>
            </div>
        </div>
    );
}
