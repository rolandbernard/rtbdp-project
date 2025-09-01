import { ThemeProvider } from "@emotion/react";
import CounterCard from "./CounterCard";
import { createTheme, CssBaseline } from "@mui/material";

const theme = createTheme({
    palette: {
        mode: "dark",
    },
});

function App() {
    return (
        <ThemeProvider theme={theme}>
            <CssBaseline />
            <CounterCard
                title="Test"
                value="500k"
                data={[
                    { x: new Date("2025-09-05T07:05:00Z"), y: 1 },
                    { x: new Date("2025-09-05T07:10:00Z"), y: 2 },
                    { x: new Date("2025-09-05T07:15:00Z"), y: 1 },
                    { x: new Date("2025-09-05T07:20:00Z"), y: 3 },
                    { x: new Date("2025-09-05T07:25:00Z"), y: 5 },
                    { x: new Date("2025-09-05T07:30:00Z"), y: 1 },
                ]}
            />
        </ThemeProvider>
    );
}

export default App;
