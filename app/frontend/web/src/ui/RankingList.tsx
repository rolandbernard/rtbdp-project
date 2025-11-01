import { useMemo, useState, type ReactNode } from "react";

import type { RankingRow, RankingTable } from "../api/ranking";
import type { Row } from "../api/client";
import { useTable } from "../api/hooks";
import { sort } from "../util";

interface ViewProps<R> {
    table: RankingTable<R>;
    from: number;
    to: number;
    rows: (row: Row<RankingRow<R>>) => ReactNode;
}

function RankingView<R>(props: ViewProps<R>) {
    const rawResults = useTable(props.table.desiredRows(props.from, props.to));
    const results = useMemo(
        () => sort(rawResults, [a => a.row_number]),
        [rawResults]
    );
    return <>{results.map(props.rows)}</>;
}

interface Props<R> {
    table: RankingTable<R>;
    rows: (row: Row<RankingRow<R>>) => ReactNode;
}

export default function RankingList<R>(props: Props<R>) {
    const [start, setStart] = useState(0);
    return (
        <>
            <button
                className="ms-1 p-2 bg-gray-600 hover:cursor-pointer"
                onClick={_e => setStart(Math.max(0, start - 10))}
            >
                &lt;
            </button>
            <button
                className="ms-1 p-2 bg-gray-600 hover:cursor-pointer"
                onClick={_e => setStart(start + 10)}
            >
                &gt;
            </button>
            <table className="mx-3">
                <thead>
                    <tr>
                        <th className="pt-1">Rank</th>
                        <th className="pt-1">RepoId</th>
                        <th className="pt-1">Score</th>
                    </tr>
                </thead>
                <tbody>
                    <RankingView
                        table={props.table}
                        from={start}
                        to={start + 10}
                        rows={props.rows}
                    />
                </tbody>
            </table>
        </>
    );
}
