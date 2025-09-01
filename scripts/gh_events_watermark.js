/// This is a simple script for computing possible watermark strategies. The idea
/// is to check whether events are mostly in order, bounded out-of-order, or
/// whether they are completely random, and we will maybe have to use injest time
/// instead for processing.

import { promises as fs } from "fs";

const INPUT = "data/ghevents";

const timestamps = await fs.readdir(INPUT);
timestamps.sort((a, b) => parseInt(a) - parseInt(b));

const seen_events = new Set();
const event_times = [];
let overlap = 0;

for (const ts of timestamps) {
    try {
        const all_events = (
            await Promise.all(
                [1, 2, 3].map(async (idx) =>
                    JSON.parse(await fs.readFile(`${INPUT}/${ts}/${idx}.json`))
                )
            )
        )
            .flat()
            .reverse();
        for (const event of all_events) {
            if (!seen_events.has(event.id)) {
                seen_events.add(event.id);
                event_times.push(new Date(event.created_at).getTime());
            } else {
                overlap = overlap + 1;
            }
        }
    } catch (e) {
        console.error(`failed to load at timestamp ${ts}`);
    }
}

console.log(`overlap: ${Math.round((100 * overlap) / event_times.length)}%`);

const delays = [];
let largest_seen = 0;

for (const ev_time of event_times) {
    if (largest_seen > ev_time) {
        delays.push(largest_seen - ev_time);
    } else {
        delays.push(0);
        largest_seen = ev_time;
    }
}

delays.sort((a, b) => a - b);
console.log(
    `delay 90th percentile: ${delays[Math.floor((delays.length * 9) / 10)]}`
);
console.log(
    `delay 99th percentile: ${delays[Math.floor((delays.length * 99) / 100)]}`
);
console.log(
    `delay 99.75th percentile: ${
        delays[Math.floor((delays.length * 997.5) / 1000)]
    }`
);
console.log(
    `delay 99.9th percentile: ${
        delays[Math.floor((delays.length * 999) / 1000)]
    }`
);
console.log(`delay maximum: ${delays[delays.length - 1]}`);
console.log(
    `delay below 1s: ${
        Math.round(
            (100_00 * delays.findIndex((v) => v >= 1_000)) / delays.length
        ) / 100
    }%`
);
console.log(
    `delay below 5s: ${
        Math.round(
            (100_00 * delays.findIndex((v) => v >= 5_000)) / delays.length
        ) / 100
    }%`
);
console.log(
    `delay below 10s: ${
        Math.round(
            (100_00 * delays.findIndex((v) => v >= 10_000)) / delays.length
        ) / 100
    }%`
);

// # Results
// overlap: 108%
// delay 90th percentile: 0
// delay 99th percentile: 1000
// delay 99.75th percentile: 6000
// delay 99.9th percentile: 280000
// delay maximum: 3720617000
// delay below 1s: 95.11%
// delay below 5s: 99.72%
// delay below 10s: 99.8%
