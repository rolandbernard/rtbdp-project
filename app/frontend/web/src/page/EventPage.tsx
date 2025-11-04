import { useParams } from "react-router";

export default function EventPage() {
    const params = useParams();
    const kind = params["kind"]!;
    return <div className="flex flex-col min-h-0 grow">Event Page {kind}</div>;
}
