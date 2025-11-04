import { StrictMode } from "react";
import { createHashRouter } from "react-router";
import { RouterProvider } from "react-router/dom";
import { createRoot } from "react-dom/client";

import Root from "./routes/Root";
import Dashboard from "./routes/Dashboard";
import ErrorPage from "./routes/ErrorPage";
import ExtraPage from "./routes/ExtraPage";
import EventPage from "./routes/EventPage";
import UserPage from "./routes/UserPage";
import RepoPage from "./routes/RepoPage";

import "./api/client";
import "./styles.css";

const router = createHashRouter([
    {
        element: <Root />,
        errorElement: <ErrorPage />,
        children: [
            { index: true, element: <Dashboard /> },
            {
                path: "/extra",
                element: <ExtraPage />,
            },
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
