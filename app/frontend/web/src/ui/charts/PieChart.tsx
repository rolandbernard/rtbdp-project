import {
    ResponsiveContainer,
    Tooltip,
    Pie,
    PieChart as RePieChart,
    Cell,
    type PieLabelRenderProps,
    Legend,
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
}

export default function PieChart(props: Props) {
    const data = sort(props.data, [e => e.x]);
    return (
        <ResponsiveContainer width="100%" height="100%">
            <RePieChart data={data}>
                <Tooltip content={<CatTooltip />} />
                <Legend />
                <Pie
                    type="monotone"
                    dataKey="y"
                    nameKey="x"
                    animationDuration={200}
                    animationEasing="linear"
                    label={labels}
                    labelLine={false}
                >
                    {data.map(entry => {
                        const opacity =
                            props.highligh && props.highligh !== entry.x;
                        return (
                            <Cell
                                key={entry.x}
                                stroke={opacity ? "none" : "#ffffff"}
                                fill={colorFor(
                                    entry.x,
                                    opacity ? "0.5" : "1.0"
                                )}
                                className="hover:transform-[scale(1.05)] active:transform-[scale(1.05)] origin-center transform-[scale(1)] transform-stroke"
                            />
                        );
                    })}
                </Pie>
            </RePieChart>
        </ResponsiveContainer>
    );
}
