import {
    ResponsiveContainer,
    Tooltip,
    Pie,
    PieChart as RePieChart,
    Cell,
    Legend,
    type PieLabelRenderProps,
} from "recharts";

import CatTooltip from "./CatTooltip";
import { colorFor, sort } from "../../util";

function labels({ percent }: PieLabelRenderProps) {
    if (percent && percent > 0.01) {
        return `${((percent ?? 1) * 100).toFixed(0)}%`;
    }
}

interface Props {
    data: { x: string; y: number }[];
    highligh?: string;
    className?: string;
    onClick?: (v: string) => void;
}

export default function PieChart(props: Props) {
    const data = sort(props.data, [e => e.x]);
    return (
        <ResponsiveContainer
            width="100%"
            height="100%"
            className={"select-none contain-strict " + (props.className ?? "")}
        >
            <RePieChart data={data}>
                <Tooltip content={<CatTooltip />} />
                <Legend />
                <Pie
                    type="monotone"
                    dataKey="y"
                    nameKey="x"
                    label={labels}
                    labelLine={false}
                    isAnimationActive={false}
                >
                    {data.map(entry => {
                        const opacity =
                            props.highligh && props.highligh !== entry.x;
                        return (
                            <Cell
                                key={entry.x}
                                stroke={opacity ? "none" : "#ffffff"}
                                fill={colorFor(entry.x, opacity ? 0.5 : 1.0)}
                                className="cursor-pointer"
                                onClick={() => props.onClick?.(entry.x)}
                            />
                        );
                    })}
                </Pie>
            </RePieChart>
        </ResponsiveContainer>
    );
}
