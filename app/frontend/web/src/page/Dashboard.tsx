import EventCounts from "../ui/EventCounts";
import EventList from "../ui/EventList";
import Rankings from "../ui/Rankings";

export default function Dashboard() {
    return (
        <div className="flex flex-col min-h-0 grow">
            <EventCounts />
            <div className="flex flex-col md:flex-row min-h-0 min-w-0 grow">
                <Rankings />
                <EventList />
            </div>
        </div>
    );
}
