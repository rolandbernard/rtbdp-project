import { formatDate } from "../../util";

interface TooltipProps {
    active?: boolean;
    payload?: { value: number }[];
    label?: Date;
    window?: number;
    start?: Date;
    stop?: Date;
}

export default function TimeTooltip(props: TooltipProps) {
    if (props.active && props.payload && props.payload.length) {
        return (
            <div className="custom-tooltip p-3 rounded-box bg-base-300/75 shadow-xl backdrop-blur-md hidden lg:block">
                <p className="label">{`${formatDate(
                    props.label!,
                    props.window ?? 0,
                    props.start && props.stop
                        ? props.stop.getTime() - props.start.getTime()
                        : undefined
                )} : ${props.payload[0]?.value}`}</p>
            </div>
        );
    }
    return null;
}
