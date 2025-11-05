import type { ReactNode } from "react";

interface Props<O extends string> {
    options: Record<O, ReactNode>;
    name?: string;
    group?: string;
    onChange?: (value: O) => void;
    value?: string;
    className?: string;
}

export default function Selector<O extends string>(props: Props<O>) {
    return (
        <div
            className={
                "flex flex-row outline-none rounded-field " +
                "focus-visible:border-primary select-none " +
                props.className
            }
        >
            {Object.entries(props.options).map(([key, node], i) => (
                <div key={key} className="w-full has-checked:z-1 ml-[-2px]">
                    <input
                        type="radio"
                        id={
                            (props.name ?? "") +
                            "-option-" +
                            key +
                            "-" +
                            props.group
                        }
                        name={props.name}
                        value={key}
                        className="peer hidden"
                        checked={props.value === key}
                        onChange={e => {
                            if (e.target.checked) {
                                props.onChange?.(key as O);
                            }
                        }}
                    />
                    <label
                        htmlFor={
                            (props.name ?? "") +
                            "-option-" +
                            key +
                            "-" +
                            props.group
                        }
                        className={
                            "inline-flex items-center justify-center w-full p-0.75 cursor-pointer peer-checked:text-primary " +
                            "peer-checked:bg-primary/15 peer-checked:border-primary peer-checked:hover:bg-primary/25 " +
                            "border-2 border-border hover:bg-content/10 dark:hover:bg-content/15 " +
                            (i === 0 ? "rounded-l-field " : "") +
                            (i + 1 === Object.entries(props.options).length
                                ? "rounded-r-field "
                                : "")
                        }
                    >
                        {node as ReactNode}
                    </label>
                </div>
            ))}
        </div>
    );
}
