import { useSearchParams } from "react-router";

import RepoRanking from "./RepoRanking";
import UserRanking from "./UserRanking";
import Selector from "./Selector";

export default function Rankings() {
    const [searchParams, setSearchParams] = useSearchParams();
    const kind = searchParams.get("ranking") ?? "repo";
    return (
        <div className="flex-1 xl:flex-2 md:w-full m-2 mr-0 p-2 xl:pt-0 flex flex-col border border-border/50 rounded-box min-w-0">
            <div className="text-sm flex flex-row items-center justify-end pt-0.5 px-1 xl:hidden">
                <div className="pe-3 text-xs whitespace-nowrap">Ranking of</div>
                <Selector
                    className="w-full max-w-64 md:w-2/3 lg:w-1/2"
                    name="user-repo-ranking"
                    options={{ user: "Users", repo: "Repositories" }}
                    value={kind === "user" ? "user" : "repo"}
                    onChange={value =>
                        setSearchParams(p => {
                            p.set("ranking", value);
                            return p;
                        })
                    }
                />
            </div>
            <div className="grow not-md:h-[50dvh] min-w-0 min-h-0 flex flex-row">
                <div
                    className={
                        "w-full min-w-0 flex flex-col" +
                        (kind === "user" ? "" : " not-xl:hidden")
                    }
                >
                    <UserRanking />
                </div>
                <div className="w-0 flex justify-center items-center not-xl:hidden">
                    <div className="w-0 h-2/3 border-r border-border/50" />
                </div>
                <div
                    className={
                        "w-full min-w-0 flex flex-col" +
                        (kind === "user" ? " not-xl:hidden" : "")
                    }
                >
                    <RepoRanking />
                </div>
            </div>
        </div>
    );
}
