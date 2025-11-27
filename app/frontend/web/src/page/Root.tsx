import { Outlet, ScrollRestoration } from "react-router";
import Header from "../ui/Header";

export default function Root() {
    return (
        <div className="md:h-dvh overflow-x-hidden flex flex-col select-none">
            <Header />
            <Outlet />
            <ScrollRestoration />
        </div>
    );
}
