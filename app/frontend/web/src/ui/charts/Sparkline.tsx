import { AreaChart, Area, ResponsiveContainer, XAxis, Tooltip } from "recharts";

import TimeTooltip from "./TimeTooltip";

interface Props {
    data: { x: Date; y: number }[];
    chartColor: string;
    window?: number;
}

export default function Sparkline(props: Props) {
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
                <XAxis dataKey="x" hide />
                <Tooltip
                    content={<TimeTooltip window={props.window} small={true} />}
                />
                <Area
                    type="monotone"
                    dataKey="y"
                    stroke={props.chartColor}
                    strokeWidth={2}
                    fill="url(#colorGradient)"
                    animationDuration={200}
                    animationEasing="linear"
                />
            </AreaChart>
        </ResponsiveContainer>
    );
}
