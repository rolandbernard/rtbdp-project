import { useState } from "react";
import { Link } from "react-router";
import { Search } from "lucide-react";

import { useClickOutside } from "../hooks";

import GlobalSearch from "./GlobalSearch";
import ThemeSelector from "./ThemeSelector";

export default function Header() {
    const [showSearch, setShowSearch] = useState(false);
    useClickOutside("nav#header", () => setShowSearch(false));
    return (
        <>
            <div className="w-full h-16 shrink-0 grow-0" />
            <nav
                id="header"
                className="bg-base-200/60 backdrop-blur-md fixed w-full z-20 top-0 start-0"
            >
                <div className="max-w-screen-xl flex items-center justify-between mx-auto">
                    <Link to="/" className="flex items-center space-x-3 ps-4">
                        <img
                            src="/favicon.svg"
                            className="h-9"
                            alt="App Logo"
                        />
                        <span className="self-center text-l font-semibold whitespace-nowrap">
                            Real-Time GitHub Events
                        </span>
                    </Link>
                    <div className="flex items-center justify-end">
                        <button
                            type="button"
                            aria-controls="navbar-search"
                            aria-expanded={showSearch}
                            id="search-toggle"
                            className="flex items-center justify-center md:hidden w-10 h-10 cursor-pointer rounded-box
                                hover:bg-content/10 dark:hover:bg-content/15 border border-transparent
                                active:border-content/20"
                            onClick={() => setShowSearch(true)}
                        >
                            <Search className="w-5 h-5" />
                            <span className="sr-only">Search</span>
                        </button>
                        <div className="w-100 hidden md:block">
                            <GlobalSearch />
                        </div>
                        <ThemeSelector />
                    </div>
                </div>
                {showSearch ? (
                    <div className="w-full px-2 pb-3" id="navbar-search">
                        <GlobalSearch autoFocus={true} />
                    </div>
                ) : undefined}
            </nav>
        </>
    );
}
