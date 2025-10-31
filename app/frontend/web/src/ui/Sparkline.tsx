import { AreaChart, Area, ResponsiveContainer, XAxis, Tooltip } from "recharts";

function twoDigitString(number: number) {
    if (number < 10) {
        return "0" + number;
    } else {
        return number.toString();
    }
}

function formatDate(date: Date, window: number) {
    return (
        twoDigitString(date.getHours()) +
        ":" +
        twoDigitString(date.getMinutes()) +
        (window >= 60
            ? "-" +
              twoDigitString((date.getMinutes() + Math.trunc(window / 60)) % 60)
            : ":" +
              twoDigitString(date.getSeconds()) +
              (window >= 1
                  ? "-" + twoDigitString((date.getSeconds() + window) % 60)
                  : ""))
    );
}

interface TooltipProps {
    active?: boolean;
    payload?: { value: number }[];
    label?: Date;
    window?: number;
}

function CustomTooltip(props: TooltipProps) {
    if (props.active && props.payload && props.payload.length) {
        return (
            <div className="custom-tooltip p-3 rounded-box bg-base-300/75 shadow-xl backdrop-blur-md hidden lg:block">
                <p className="label">{`${formatDate(
                    props.label!,
                    props.window ?? 0
                )} : ${props.payload[0]?.value}`}</p>
            </div>
        );
    }
    return null;
}

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
                <Tooltip content={<CustomTooltip window={props.window} />} />
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
