import { Card, CardContent, CardMedia } from "@mui/material";
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
        <Card variant="outlined" sx={{ display: "flex", width: "fit-content" }}>
            <CardContent sx={{ alignContent: "center" }}>
                <Typography variant="caption" color="text.secondary">
                    {title}
                </Typography>
                <Typography variant="h5">{value}</Typography>
            </CardContent>
            <CardMedia>
                <SparkLineChart
                    width={200}
                    height={100}
                    area
                    showTooltip
                    showHighlight
                    disableClipping
                    baseline="min"
                    curve="natural"
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
                        data: data.map((e) => e.x),
                    }}
                />
            </CardMedia>
        </Card>
    );
}
