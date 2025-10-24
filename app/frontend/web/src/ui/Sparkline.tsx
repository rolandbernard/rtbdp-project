import { AreaChart, Area, ResponsiveContainer, XAxis, Tooltip } from "recharts";

function twoDigitString(number: number) {
    if (number < 10) {
        return "0" + number;
    } else {
        return number.toString();
    }
}

function formatDate(date: Date) {
    const MONTH_NAME = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ];
    return (
        MONTH_NAME[date.getMonth()] +
        " " +
        (date.getDay() + 1) +
        ", " +
        twoDigitString(date.getHours()) +
        ":" +
        twoDigitString(date.getMinutes())
    );
}

interface TooltipProps {
    active?: boolean;
    payload?: { value: number }[];
    label?: Date;
}

function CustomTooltip(props: TooltipProps) {
    if (props.active && props.payload && props.payload.length) {
        return (
            <div className="custom-tooltip p-3 rounded-box bg-base-300/75 shadow-xl backdrop-blur-md hidden lg:block">
                <p className="label">{`${formatDate(props.label!)} : ${
                    props.payload[0]?.value
                }`}</p>
            </div>
        );
    }
    return null;
}

interface Props {
    data: { x: Date; y: number }[];
    chartColor: string;
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
                <Tooltip content={<CustomTooltip />} />
                <Area
                    type="monotone"
                    dataKey="y"
                    stroke={props.chartColor}
                    strokeWidth={2}
                    fill="url(#colorGradient)"
                    activeDot={false}
                    dot={false}
                />
            </AreaChart>
        </ResponsiveContainer>
    );
}
