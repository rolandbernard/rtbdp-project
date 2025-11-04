import { useParams } from "react-router";

export default function RepoPage() {
    const params = useParams();
    const repoId = params["repoId"]!;
    return <div className="flex flex-col min-h-0 grow">Repo Page {repoId}</div>;
}
