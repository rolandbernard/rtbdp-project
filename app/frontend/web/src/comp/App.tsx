import EventCounts from "./EventCounts";
import EventList from "./EventList";
import SearchBar from "./SearchBar";

export default function App() {
    return (
        <div>
            <SearchBar></SearchBar>
            <EventCounts></EventCounts>
            <EventList></EventList>
        </div>
    );
}
