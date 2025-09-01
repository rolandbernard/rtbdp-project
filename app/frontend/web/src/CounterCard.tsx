import { Card, CardContent } from "@mui/material";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import {
    areaElementClasses,
    chartsAxisHighlightClasses,
    lineElementClasses,
} from "@mui/x-charts";
import { SparkLineChart } from "@mui/x-charts/SparkLineChart";

interface CounterCardProps {
    title: string;
    value: string;
    data: { x: Date; y: number }[];
}

export default function CounterCard({ title, value, data }: CounterCardProps) {
    return (
        <Card variant="outlined" sx={{ height: 'fit-content' }}>
            <CardContent
                sx={{
                    padding: { xs: 1, md: 1.5 },
                    "&:last-child": { paddingBottom: { xs: 1, md: 1.5 } },
                }}
            >
                <Stack direction="row">
                    <Stack direction="column">
                        <Typography variant="caption" color="text.secondary">
                            {title}
                        </Typography>
                        <Typography variant="h5">{value}</Typography>
                    </Stack>
                    <SparkLineChart
                        height={40}
                        width={195}
                        area
                        showTooltip
                        showHighlight
                        axisHighlight={{ x: "line" }}
                        sx={{
                            [`& .${areaElementClasses.root}`]: { opacity: 0.2 },
                            [`& .${lineElementClasses.root}`]: {
                                strokeWidth: 3,
                            },
                            [`& .${chartsAxisHighlightClasses.root}`]: {
                                stroke: "rgb(137, 86, 255)",
                                strokeDasharray: "none",
                                strokeWidth: 2,
                            },
                        }}
                        data={data.map((e) => e.y)}
                        xAxis={{
                            scaleType: "band",
                            data: data.map((e) => e.x),
                        }}
                    />
                </Stack>
            </CardContent>
        </Card>
    );
}
