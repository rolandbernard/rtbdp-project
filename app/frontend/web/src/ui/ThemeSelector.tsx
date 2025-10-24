import { Moon, Settings, Sun } from "lucide-react";
import React, { useEffect, useState } from "react";

const OPTIONS = {
    light: Sun,
    dark: Moon,
    system: Settings,
};

export default function ThemeSelector() {
    const [show, setShow] = useState(false);
    const [selected, setSelected] = useState(localStorage.theme ?? "system");
    useEffect(() => {
        const handler = () => setSelected(localStorage.theme ?? "system");
        addEventListener("storage", handler);
        return () => removeEventListener("storage", handler);
    }, []);
    const setupTheme = (value: string) => {
        if (value) {
            if (value === "system") {
                delete localStorage.theme;
            } else {
                localStorage.theme = value;
            }
        }
        setSelected(value);
        document.documentElement.classList.toggle(
            "dark",
            "theme" in localStorage
                ? localStorage.theme === "dark"
                : matchMedia("(prefers-color-scheme: dark)").matches
        );
    };
    return (
        <div
            id="theme-select"
            className="relative max-w-screen-xl flex items-center justify-between mx-auto p-3"
            onBlur={e => {
                if (!e.relatedTarget?.closest("div#theme-select")) {
                    setShow(false);
                }
            }}
        >
            <button
                type="button"
                aria-controls="theme-select"
                aria-expanded={show}
                id="search-toggle"
                className="
                    flex items-center justify-center w-10 h-10 cursor-pointer
                    rounded-box hover:bg-content/10 dark:hover:bg-content/15 border
                    border-transparent active:border-content/10"
                onClick={() => setShow(true)}
            >
                <Moon className="w-5 h-5 hidden dark:block"></Moon>
                <Sun className="w-5 h-5 block dark:hidden"></Sun>
                <span className="sr-only">Search</span>
            </button>
            {show ? (
                <div className="absolute inset-y-full end-3 z-50">
                    <div className="flex flex-col items-stretch w-full bg-base-300 shadow-xl rounded-box p-1">
                        {Object.entries(OPTIONS).map(([name, icon]) => {
                            return (
                                <button
                                    type="button"
                                    key={name}
                                    className={
                                        "flex items-center justify-start whitespace-nowrap p-2 cursor-pointer rounded-box " +
                                        "hover:bg-content/10 border border-transparent active:border-content/10 " +
                                        "dark:hover:bg-content/15 " +
                                        (selected === name
                                            ? "text-primary"
                                            : "")
                                    }
                                    onClick={() => setupTheme(name)}
                                >
                                    {React.createElement(icon, {
                                        className: "w-5 h-5 inline",
                                    })}
                                    <span className="px-4">
                                        {name[0]?.toUpperCase() + name.slice(1)}
                                    </span>
                                </button>
                            );
                        })}
                    </div>
                </div>
            ) : undefined}
        </div>
    );
}
