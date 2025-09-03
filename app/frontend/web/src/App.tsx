import { useEffect, useState } from "react";
import { useTable } from "./api";

export default function App() {
    const table = useTable("counts_live", ["window_size", "kind"], []);
    const grouped = Object.groupBy(table, row => row.kind);
    const sorted = Object.values(grouped);
    sorted.sort(
        (a, b) =>
            b!.find(r => r.window_size === "5m")?.num_events -
            a!.find(r => r.window_size === "5m")?.num_events
    );
    return (
        <table className="mx-3">
            <thead>
                <tr>
                    <th className="pt-1">Kind</th>
                    <th className="pt-1">5m</th>
                    <th className="pt-1">1h</th>
                    <th className="pt-1">6h</th>
                    <th className="pt-1">24h</th>
                </tr>
            </thead>
            <tbody>
                {sorted.map(rows => (
                    <tr key={rows![0].kind}>
                        <td>{rows![0].kind}</td>
                        {rows!
                            .toSorted(e =>
                                ["5m", "1h", "6h", "24h"].indexOf(e.window_size)
                            )
                            .map(row => (
                                <td key={row.window_size} className="pt-1 px-3">
                                    {row.num_events}
                                </td>
                            ))}
                    </tr>
                ))}
            </tbody>
        </table>
    );
}
