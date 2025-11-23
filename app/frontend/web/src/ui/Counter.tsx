import { memo } from "react";

interface LettersProps {
    value: string;
    options: string[];
    className?: string;
}

export function Letters(props: LettersProps) {
    const len = props.options.length;
    const idx = props.options.indexOf(props.value);
    return (
        <div
            className={
                "relative overflow-hidden contain-content " + props.className
            }
        >
            {props.options.map((v, i) => (
                <div
                    key={i}
                    style={{
                        transform: `rotateX(${
                            ((idx - i) * 360) / len
                        }deg) translateZ(2em)`,
                    }}
                    className="top-0 left-0 absolute backface-hidden"
                >
                    {v}
                </div>
            ))}
        </div>
    );
}

interface DigitProps {
    value?: number;
    leading: boolean;
}

function Digit(props: DigitProps) {
    return (
        <div className="relative overflow-hidden contain-paint">
            <div className="invisible">0</div>
            {[...Array(10)].map((_, i) => (
                <div
                    key={i}
                    style={{
                        willChange:
                            !props.leading && (props.value ?? 0) >= 10
                                ? "transform"
                                : undefined,
                        transform: `rotateX(${
                            -36 * i + 36 * (props.value ?? 0)
                        }deg) translateZ(2em)`,
                    }}
                    className={
                        "top-0 left-0 absolute backface-hidden " +
                        (i === 0
                            ? props.leading
                                ? "opacity-0 "
                                : "opacity-100 "
                            : "")
                    }
                >
                    {props.value == null && i === 0 ? "–" : i}
                </div>
            ))}
        </div>
    );
}

const MemoDigit = memo(Digit);

function numDigits(value: number) {
    let cnt = 1;
    while (value >= 10) {
        value /= 10;
        cnt += 1;
    }
    return cnt;
}

interface Props {
    value?: number;
    maxDigits?: number;
    className?: string;
}

export default function Counter(props: Props) {
    const digits = props.maxDigits ?? numDigits(props.value ?? 0) + 1;
    return (
        <div
            className={
                "relative inline-block contain-paint " + (props.className ?? "")
            }
        >
            <div className="absolute top-0 right-0 text-transparent">
                {props.value}
            </div>
            <div
                className="flex flex-row justify-end pointer-events-none select-none"
                aria-hidden="true"
            >
                {[...Array(digits)]
                    .map((_, i) => (
                        <MemoDigit
                            key={i}
                            value={
                                props.value != null
                                    ? Math.trunc(props.value / 10 ** i)
                                    : undefined
                            }
                            leading={i != 0 && (props.value ?? 0) < 10 ** i}
                        />
                    ))
                    .reverse()}
            </div>
        </div>
    );
}
