import { StrictMode } from "react";
import { createHashRouter } from "react-router";
import { RouterProvider } from "react-router/dom";
import { createRoot } from "react-dom/client";

import Root from "./page/Root";
import Dashboard from "./page/Dashboard";
import ErrorPage from "./page/ErrorPage";
import EventPage from "./page/EventPage";
import UserPage from "./page/UserPage";
import RepoPage from "./page/RepoPage";

import "./api/client";
import "./styles.css";

const originalWarn = console.warn;
console.warn = (...data: unknown[]) => {
    if (
        typeof data[0] === "string" &&
        data[0].includes("of chart should be greater than 0")
    ) {
        // These are false warnings from Recharts whenever a new chart component
        // is mounted. These make the output very noisy so we ignore them.
        return;
    }
    originalWarn(...data);
};

const router = createHashRouter([
    {
        element: <Root />,
        errorElement: <ErrorPage />,
        children: [
            { index: true, element: <Dashboard /> },
            {
                path: "/event/:kind",
                element: <EventPage />,
            },
            {
                path: "/user/:userId",
                element: <UserPage />,
            },
            {
                path: "/repo/:repoId",
                element: <RepoPage />,
            },
        ],
    },
]);

createRoot(document.getElementById("root")!).render(
    <StrictMode>
        <RouterProvider router={router} />
    </StrictMode>
);
