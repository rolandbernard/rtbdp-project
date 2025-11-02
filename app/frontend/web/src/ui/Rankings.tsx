import { useState } from "react";

import RepoRanking from "./RepoRanking";
import UserRanking from "./UserRanking";
import Selector from "./Selector";

export default function Rankings() {
    const [forUsers, setForUsers] = useState(true);
    return (
        <div className="md:w-full m-2 mr-0 p-2 xl:pt-0 flex flex-col border border-border/50 rounded-box min-w-0">
            <div className="text-sm flex flex-row items-center justify-end pt-0.5 px-1 xl:hidden">
                <div className="pe-3 text-xs whitespace-nowrap">Ranking of</div>
                <Selector
                    className="w-full max-w-64 md:w-2/3 lg:w-1/2"
                    name="user-repo-ranking"
                    options={{ users: "Users", repos: "Repositories" }}
                    value={forUsers ? "users" : "repos"}
                    onChange={value => {
                        console.log(value);
                        setForUsers(value === "users");
                    }}
                />
            </div>
            <div className="grow not-md:h-[50dvh] min-w-0 min-h-0 flex flex-row gap-8">
                <div
                    className={
                        "w-full min-w-0 flex flex-col" +
                        (forUsers ? "" : " not-xl:hidden")
                    }
                >
                    <UserRanking />
                </div>
                <div
                    className={
                        "w-full min-w-0 flex flex-col" +
                        (forUsers ? " not-xl:hidden" : "")
                    }
                >
                    <RepoRanking />
                </div>
            </div>
        </div>
    );
}
