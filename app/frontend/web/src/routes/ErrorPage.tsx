import { Link, useRouteError } from "react-router";

import Header from "../ui/Header";

export default function ErrorPage() {
    const error = useRouteError() as { status: number; statusText: string };
    return (
        <div className="md:h-dvh overflow-x-hidden flex flex-col">
            <Header />
            <div className="flex flex-col min-h-0 grow justify-center items-center">
                <div className="text-5xl pb-8">
                    {error.status} {error.statusText}
                </div>
                <Link to="/" className="text-sm text-primary underline">
                    Get Back to Home
                </Link>
            </div>
        </div>
    );
}
