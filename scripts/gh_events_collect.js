/// This is a simple script for collecting from the GitHub Events API some events
/// and store them into `data/ghevents/`. Collects all of the 300 events exposed
/// by the API every 2.25 seconds. This is just under the rate limit.

import { promises as fs } from 'fs';

const GITHUB_UTL = 'https://api.github.com/events';
const OUTPUT = 'data/ghevents';

setInterval(async () => {
    const ts = Date.now();
    console.log(`loading for ${ts}...`);
    await Promise.allSettled([1, 2, 3].map(async (page) => {
        const response = await fetch(`${GITHUB_UTL}?per_page=100&page=${page}`, {
            headers: {
                'Accept': 'application/vnd.github+json',
                'X-GitHub-Api-Version': '2022-11-28',
                'Authorization': `token ${process.env.GITHUB_TOKEN}`,
            }
        });
        console.log(`limit remaining: ${response.headers.get('x-ratelimit-remaining')}`);
        const image = await response.arrayBuffer();
        await fs.mkdir(`${OUTPUT}/${ts}`, { recursive: true });
        await fs.writeFile(`${OUTPUT}/${ts}/${page}.json`, Buffer.from(image));
    }));
    console.log(`loaded for ${ts}.`);
}, 2_250)
