import { isRouteErrorResponse, Link, useRouteError } from "react-router";

import Header from "../ui/Header";

export default function ErrorPage() {
    const error = useRouteError();
    return (
        <div className="md:h-dvh overflow-x-hidden flex flex-col">
            <Header />
            <div className="flex flex-col min-h-0 grow justify-center items-center">
                <div className="text-5xl pb-8 flex flex-col items-center max-w-[50vw]">
                    {isRouteErrorResponse(error) ? (
                        <>
                            <div className="pb-1">
                                {error.status} {error.statusText}
                            </div>
                            <p className="text-sm">{error.data}</p>
                        </>
                    ) : error instanceof Error ? (
                        <>
                            <div className="pb-1">Internal Error</div>
                            <p className="text-sm pb-4">{error.message}</p>
                            <pre className="text-xs max-h-[50vh] overflow-scroll max-w-[50vw]">
                                {error.stack}
                            </pre>
                        </>
                    ) : error instanceof Response ? (
                        <>
                            <div className="pb-1">
                                {error.status} {error.statusText}
                            </div>
                        </>
                    ) : (
                        "Unknown Error"
                    )}
                </div>
                <Link
                    viewTransition
                    to="/"
                    className="text-sm text-primary underline dark:hover:text-primary/90 hover:text-primary/75"
                >
                    Get Back to Home
                </Link>
            </div>
        </div>
    );
}
