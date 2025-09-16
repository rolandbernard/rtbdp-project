import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import App from "./comp/App.tsx";

import "./api/client.ts";
import "./styles.css";

createRoot(document.getElementById("root")!).render(
    <StrictMode>
        <App />
    </StrictMode>
);
