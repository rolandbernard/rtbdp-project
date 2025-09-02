import { ThemeProvider } from "@emotion/react";
import CounterCard from "./CounterCard";
import { createTheme, CssBaseline } from "@mui/material";
import { useEffect, useState } from "react";

const theme = createTheme({
    palette: {
        mode: "dark",
    },
});

export default function App() {
    const [data, setData] = useState([
        { x: new Date("2025-09-05T07:05:00Z"), y: 1 },
        { x: new Date("2025-09-05T07:10:00Z"), y: 2 },
        { x: new Date("2025-09-05T07:15:00Z"), y: 1 },
        { x: new Date("2025-09-05T07:20:00Z"), y: 3 },
        { x: new Date("2025-09-05T07:25:00Z"), y: 5 },
        { x: new Date("2025-09-05T07:30:00Z"), y: 1 },
        { x: new Date("2025-09-05T07:35:00Z"), y: 2 },
        { x: new Date("2025-09-05T07:40:00Z"), y: 2 },
        { x: new Date("2025-09-05T07:45:00Z"), y: 3 },
        { x: new Date("2025-09-05T07:50:00Z"), y: 3 },
        { x: new Date("2025-09-05T07:55:00Z"), y: 2 },
    ]);
    useEffect(() => {
        const interval = setInterval(() => {
            const { x, y } = data[data.length - 1];
            setData([
                ...data.slice(1),
                {
                    x: new Date(x.getTime() + 5 * 60 * 1_000),
                    y: y + Math.random() - 0.5,
                },
            ]);
        }, 1000);
        return () => clearInterval(interval);
    }, [data]);
    return (
        <ThemeProvider theme={theme}>
            <CssBaseline />
            <CounterCard title="Test" value="500k" data={data} />
        </ThemeProvider>
    );
}
