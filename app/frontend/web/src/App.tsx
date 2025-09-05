import { countsRanking, groupBy, sorted, useTable } from "./api";

export default function App() {
    const result =
        useTable(countsRanking, o =>
            sorted(
                groupBy(o, "kind").map(rs =>
                    sorted(rs, e =>
                        ["5m", "1h", "6h", "24h"].indexOf(e.window_size)
                    )
                ),
                e => -e[0]!.num_events
            )
        ) ?? [];
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
                {result.map(rows => (
                    <tr key={rows[0]!.kind}>
                        <td>{rows[0]!.kind}</td>
                        {rows.map(row => (
                            <td key={row.window_size} className="pt-1 px-3">
                                {row.num_events} ({row.rank})
                            </td>
                        ))}
                    </tr>
                ))}
            </tbody>
        </table>
    );
}
