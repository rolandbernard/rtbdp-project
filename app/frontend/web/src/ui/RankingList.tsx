import { useMemo, type ReactNode } from "react";

import type { RankingRow, RankingTable } from "../api/ranking";
import type { Row } from "../api/client";
import { useTable } from "../api/hooks";
import { sort } from "../util";

interface Props<R> {
    table: RankingTable<R>;
    from: number;
    to: number;
    rows: (row: Row<RankingRow<R>>) => ReactNode;
}

export default function RankingList<R>(props: Props<R>) {
    const rawResults = useTable(props.table.desiredRows(props.from, props.to));
    const results = useMemo(
        () => sort(rawResults, [a => a.row_number]),
        [rawResults]
    );
    return <>{results.map(props.rows)}</>;
}
