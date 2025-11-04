import { useParams } from "react-router";

export default function UserPage() {
    const params = useParams();
    const userId = params["userId"]!;
    return <div className="flex flex-col min-h-0 grow">User Page {userId}</div>;
}
