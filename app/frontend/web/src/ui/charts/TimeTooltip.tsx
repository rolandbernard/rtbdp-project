import { formatDate } from "../../util";

export interface TooltipProps {
    active?: boolean;
    payload?: { value: number; name?: string; fill?: string }[];
    label?: Date;
    window?: number;
    start?: Date;
    stop?: Date;
}

export default function TimeTooltip(props: TooltipProps) {
    if (props.active && props.payload) {
        if (props.payload.length == 1) {
            return (
                <div className="custom-tooltip p-3 rounded-box bg-base-300/75 shadow-xl backdrop-blur-md">
                    <p className="label">
                        {formatDate(
                            props.label!,
                            props.window ?? 0,
                            props.start && props.stop
                                ? props.stop.getTime() - props.start.getTime()
                                : undefined
                        )}{" "}
                        : {props.payload![0]?.value}
                    </p>
                </div>
            );
        } else {
            return (
                <div className="custom-tooltip p-3 rounded-box bg-base-300/75 shadow-xl backdrop-blur-md">
                    <p className="label">
                        {formatDate(
                            props.label!,
                            props.window ?? 0,
                            props.start && props.stop
                                ? props.stop.getTime() - props.start.getTime()
                                : undefined
                        )}
                    </p>
                    {props.payload.reverse().map(each => (
                        <p
                            className="label ps-2"
                            key={each.name}
                            style={{ color: each.fill }}
                        >
                            {each.name} : {each.value}
                        </p>
                    ))}
                </div>
            );
        }
    }
    return null;
}
