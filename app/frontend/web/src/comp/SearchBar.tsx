import { useMemo, useState } from "react";
import { useTable } from "../api/client";
import { users, repos } from "../api/tables";
import { sortedKey, useDebounce } from "../api/util";

function boldQuery(text: string, query: string) {
    const index = text.toLowerCase().indexOf(query.toLowerCase());
    if (index == -1) {
        return <span>{text}</span>;
    } else {
        return (
            <>
                <span>{text.substring(0, index)}</span>
                <span className="font-bold">
                    {text.substring(index, index + query.length)}
                </span>
                <span>{text.substring(index + query.length)}</span>
            </>
        );
    }
}

export default function SearchBar() {
    const [query, setQuery] = useState("");
    const debounced = useDebounce(query.toLowerCase(), 250);
    const userResults = useTable(
        users.where("username", { substr: debounced }).limit(10)
    );
    const repoResults = useTable(
        repos
            .where("reponame", { substr: debounced })
            .or()
            .where("fullname", { substr: debounced })
            .limit(10)
    );
    const results = useMemo(() => {
        const combined = [];
        for (const user of userResults ?? []) {
            combined.push({
                ...user,
                name: user.username,
                kind: user.user_type ?? "User",
            });
        }
        for (const repo of repoResults ?? []) {
            combined.push({
                ...repo,
                name: repo.fullname ?? repo.reponame,
                kind: "Repo",
            });
        }
        combined.sort(sortedKey([e => e.name?.length ?? 0]));
        combined.slice(10);
        return combined;
    }, [userResults, repoResults]);
    return (
        <>
            <input
                className="m-3 border p-1"
                type="text"
                value={query}
                onChange={e => setQuery(e.target.value)}
            ></input>
            <table className="mx-3">
                <thead>
                    <tr>
                        <th className="pt-1">Id</th>
                        <th className="pt-1">Name</th>
                        <th className="pt-1">Type</th>
                    </tr>
                </thead>
                <tbody>
                    {results.map(rows => {
                        return (
                            <tr key={rows.id}>
                                <td className="pt-1 px-3">{rows.id}</td>
                                <td className="pt-1 px-3">
                                    {boldQuery(rows.name!, query)}
                                </td>
                                <td className="pt-1 px-3">{rows.kind}</td>
                            </tr>
                        );
                    })}
                </tbody>
            </table>
        </>
    );
}
