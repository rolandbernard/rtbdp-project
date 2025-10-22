interface DigitProps {
    value: number;
    leading: boolean;
}

function Digit(props: DigitProps) {
    return (
        <div className="relative overflow-hidden">
            <div className="invisible">0</div>
            {[...Array(10)].map((_, i) => (
                <div
                    key={i}
                    style={{
                        transform: `rotateX(${
                            -36 * i + 36 * props.value
                        }deg) translateZ(2em)`,
                    }}
                    className={
                        "top-0 left-0 absolute backface-hidden " +
                        (i === 0 && props.leading
                            ? "opacity-0 "
                            : "opacity-100 ")
                    }
                >
                    {i}
                </div>
            ))}
        </div>
    );
}

interface Props {
    value: number;
    maxDigits: number;
    className?: string;
}

export default function Counter(props: Props) {
    return (
        <div className={"relative inline-block " + (props.className ?? "")}>
            <div className="absolute top-0 right-0 text-transparent">
                {props.value}
            </div>
            <div
                className="flex flex-row pointer-events-none select-none"
                aria-hidden="true"
            >
                {[...Array(props.maxDigits)]
                    .map((_, i) => (
                        <Digit
                            key={i}
                            value={Math.trunc(props.value / 10 ** i)}
                            leading={i != 0 && props.value < 10 ** i}
                        ></Digit>
                    ))
                    .reverse()}
            </div>
        </div>
    );
}
