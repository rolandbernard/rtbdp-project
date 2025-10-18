import { StrictMode } from "react";
import { HashRouter } from "react-router";
import { createRoot } from "react-dom/client";

import App from "./ui/App.tsx";

import "./api/client.ts";
import "./styles.css";

createRoot(document.getElementById("root")!).render(
    <StrictMode>
        <HashRouter>
            <App />
        </HashRouter>
    </StrictMode>
);
