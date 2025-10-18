import { useEffect, useRef, useState } from "react";
import { Link } from "react-router";
import { FaMagnifyingGlass } from "react-icons/fa6";

import SearchBar from "./SearchBar";

export default function Header() {
    const inputRef = useRef<HTMLInputElement>(null);
    const [showSearch, setShowSearch] = useState(false);
    useEffect(() => {
        if (showSearch && inputRef.current) {
            inputRef.current.focus();
        }
    }, [showSearch]);
    return (
        <nav
            className="bg-base-200 fixed w-full z-20 top-0 start-0"
            onBlur={e => {
                if (!e.relatedTarget?.closest("nav")) {
                    setShowSearch(false);
                }
            }}
        >
            <div className="max-w-screen-xl flex items-center justify-between mx-auto p-3">
                <Link to="/" className="flex items-center space-x-3">
                    <img src="/favicon.svg" className="h-9" alt="App Logo" />
                    <span className="self-center text-l font-semibold whitespace-nowrap">
                        Real-Time GitHub Events
                    </span>
                </Link>
                <button
                    type="button"
                    aria-controls="navbar-search"
                    aria-expanded="false"
                    id="search-toggle"
                    className="flex items-center justify-center md:hidden w-10 h-10 cursor-pointer rounded-box hover:bg-base-content/15 active:border active:border-base-content/20"
                    onClick={() => setShowSearch(true)}
                >
                    <FaMagnifyingGlass className="w-5 h-5"></FaMagnifyingGlass>
                    <span className="sr-only">Search</span>
                </button>
                <div className="w-100 hidden md:block">
                    <SearchBar inputRef={inputRef}></SearchBar>
                </div>
            </div>
            {showSearch ? (
                <div className="w-full px-2 pb-3" id="navbar-search">
                    <SearchBar inputRef={inputRef}></SearchBar>
                </div>
            ) : undefined}
        </nav>
    );
}
