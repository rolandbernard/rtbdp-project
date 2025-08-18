/// This is a simple script for compacting the data collected from the other script
/// for use in the dummy GitHub Events API server. This simply combines all of the
/// 3 files into one file of events for each of the last 1000 timestamps.

import { promises as fs } from 'fs';

const INPUT = 'data/ghevents';
const OUTPUT = 'app/ghdummy/src/main/resources/com/rolandb/ghevents';

const timestamps = (await fs.readdir(INPUT));
timestamps.sort((a, b) => parseInt(a) - parseInt(b))

const newest_times = timestamps.slice(-1_000)
const start_ts = parseInt(newest_times[0])

await fs.rm(OUTPUT, { recursive: true, force: true });
await fs.mkdir(OUTPUT, { recursive: true });
for (const ts of newest_times) {
    const all_events = (await Promise.all([1, 2, 3].map(
        async (idx) => JSON.parse(await fs.readFile(`${INPUT}/${ts}/${idx}.json`))
    ))).flat();
    const new_ts = (parseInt(ts) - start_ts).toString();
    await fs.writeFile(`${OUTPUT}/${new_ts}.json`, JSON.stringify(all_events));
}

