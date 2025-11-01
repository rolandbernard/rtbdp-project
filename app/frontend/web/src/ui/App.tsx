import EventCounts from "./EventCounts";
import EventList from "./EventList";
import Header from "./Header";
import Rankings from "./Rankings";

export default function App() {
    return (
        <div className="md:h-dvh overflow-x-hidden flex flex-col">
            <Header />
            <div className="flex flex-col min-h-0 grow">
                <EventCounts />
                <div className="flex flex-col md:flex-row min-h-0 min-w-0 grow">
                    <Rankings />
                    <EventList />
                </div>
            </div>
        </div>
    );
}
