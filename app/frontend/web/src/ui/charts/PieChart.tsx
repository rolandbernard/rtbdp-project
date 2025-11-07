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

const COLORS = ["#0088FE", "#00C49F", "#FFBB28", "#FF8042", "#8884D8"];

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
    return (
        <ResponsiveContainer width="100%" height="100%">
            <RePieChart data={props.data}>
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
                    {props.data.map((entry, index) => {
                        const opacity =
                            props.highligh && props.highligh !== entry.x
                                ? "80"
                                : "";
                        return (
                            <Cell
                                key={`cell-${entry.x}`}
                                stroke={"#ffffff" + opacity}
                                fill={COLORS[index % COLORS.length] + opacity}
                                className="hover:transform-[scale(1.05)] active:transform-[scale(1.05)] origin-center transform-[scale(1)] transform-stroke"
                            />
                        );
                    })}
                </Pie>
            </RePieChart>
        </ResponsiveContainer>
    );
}
