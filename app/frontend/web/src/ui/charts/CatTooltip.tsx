import type { EventKind } from "../../api/tables";

interface TooltipProps {
    active?: boolean;
    payload?: { value: number; name: EventKind; payload: { fill: string } }[];
}

export default function CatTooltip(props: TooltipProps) {
    if (props.active && props.payload && props.payload.length) {
        return (
            <div className="custom-tooltip p-3 rounded-box bg-base-300/75 shadow-xl backdrop-blur-md hidden lg:block">
                <p
                    className="label"
                    style={{ color: props.payload![0]?.payload.fill }}
                >
                    {props.payload[0]?.name} : {props.payload[0]?.value}
                </p>
            </div>
        );
    }
    return null;
}
