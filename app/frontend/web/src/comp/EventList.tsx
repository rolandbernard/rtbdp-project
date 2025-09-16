import { useTable } from "../api/client";
import { events } from "../api/tables";

export default function App() {
    const result = useTable(events.limit(10)) ?? [];
    return (
        <table className="mx-3">
            <thead>
                <tr>
                    <th className="pt-1">Time</th>
                    <th className="pt-1">Kind</th>
                    <th className="pt-1">Details</th>
                </tr>
            </thead>
            <tbody>
                {result.map(row => (
                    <tr key={row.id}>
                        <td className="pt-1 px-1">{row.created_at}</td>
                        <td className="pt-1 px-1">{row.kind}</td>
                        <td className="pt-1 px-3">{row.details}</td>
                    </tr>
                ))}
            </tbody>
        </table>
    );
}
