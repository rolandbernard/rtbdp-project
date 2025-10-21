import { useState } from "react";
import { Link } from "react-router";
import { FaMagnifyingGlass } from "react-icons/fa6";

import SearchBar from "./SearchBar";
import ThemeSelector from "./ThemeSelector";

export default function Header() {
    const [showSearch, setShowSearch] = useState(false);
    return (
        <>
            <div className="w-full h-16"></div>
            <nav
                id="header"
                className="bg-base-200 fixed w-full z-20 top-0 start-0"
                onBlur={e => {
                    if (!e.relatedTarget?.closest("nav#header")) {
                        setShowSearch(false);
                    }
                }}
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
                            hover:bg-base-content/15 border border-transparent active:border-base-content/20"
                            onClick={() => setShowSearch(true)}
                        >
                            <FaMagnifyingGlass className="w-5 h-5"></FaMagnifyingGlass>
                            <span className="sr-only">Search</span>
                        </button>
                        <div className="w-100 hidden md:block">
                            <SearchBar></SearchBar>
                        </div>
                        <ThemeSelector></ThemeSelector>
                    </div>
                </div>
                {showSearch ? (
                    <div className="w-full px-2 pb-3" id="navbar-search">
                        <SearchBar autoFocus={true}></SearchBar>
                    </div>
                ) : undefined}
            </nav>
        </>
    );
}
