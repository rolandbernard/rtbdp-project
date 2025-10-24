import type { ReactNode } from "react";

interface Props {
    options: Record<string, ReactNode>;
    name?: string;
    onChange?: (value: string) => void;
    value?: string;
    className?: string;
}

export default function Selector(props: Props) {
    return (
        <div
            className={
                "flex flex-row outline-none rounded-field justify-stretch " +
                "focus-visible:border-primary select-none " +
                props.className
            }
        >
            {Object.entries(props.options).map(([key, node], i) => (
                <div
                    key={key}
                    className="w-full has-checked:z-1 ml-[-2px]"
                    onClick={() => props.onChange?.(key)}
                >
                    <input
                        type="radio"
                        id={(props.name ?? "") + "-option-" + key}
                        name={props.name}
                        value={key}
                        className="peer hidden"
                        defaultChecked={
                            props.value != null
                                ? props.value === key
                                : undefined
                        }
                    />
                    <label
                        htmlFor={(props.name ?? "") + "-option-" + key}
                        className={
                            "inline-flex items-center justify-center w-full p-1 cursor-pointer peer-checked:text-primary " +
                            "peer-checked:bg-primary/15 peer-checked:border-primary peer-checked:hover:bg-primary/25 " +
                            "border-2 border-border hover:bg-content/10 dark:hover:bg-content/15 " +
                            (i === 0 ? "rounded-l-field " : "") +
                            (i + 1 === Object.entries(props.options).length
                                ? "rounded-r-field "
                                : "")
                        }
                    >
                        {node}
                    </label>
                </div>
            ))}
        </div>
    );
}
