10-15 pages, 2000-4000 words.
* Title: Real-Time Analysis of Public GitHub Activity
* Title page
  * title, authors, abstract, link to the code repository
* Application domain
  * Context and Motivation
    * Open-source software development as a global, continuously evolving ecosystem
    * GitHub as the largest public platform for collaborative coding
    * Existing dashboards mainly focus on historical data
    * Want: Immediate insights, Monitoring trends in near real-time, Detecting emerging projects
  * Use cases:
    * Monitoring project popularity
    * Observing development activity peaks
    * Discovering emerging repositories
    * Tracking user contributions
    * Studying ecosystem dynamics
  * Project Objectives
    * Functionality
      * Acquire public GitHub events continuously
      * Enrich raw event data with real-time statistics
      * Generate leaderboards of active users and repositories
      * Provide interactive web pages for users, repositories, and event types
    * Technical
      * Implement a fully distributed pipeline using: Kafka for messaging, Flink for stream processing, PostgreSQL/TimescaleDB for storage
      * Ensure low-latency processing
      * Guarantee fault tolerance and recovery
      * Provide reproducible deployment using Docker
* Description of the data sources
  * complexity, size, speed, ...
  * GitHub Events API
    * Is the primary datasource.
    * `/events` HTTP endpoint.
    * Retrieving data using periodic polling.
    * Authentication via tokens.
  * Dummy API and GHArchive
    * Usage of dummy API for testing and offline development.
    * Does not require a real GitHub API token.
    * Uses historical data from GHArchive.
  * Data Characteristics
    * Volume and Velocity
      * Thousands of events per minute: Generally hovering around 70-120 events/second depending on the time of day.
      * Results in millions of events per day.
      * 170-200MiB per hour.
      * Can be polled at different intervals. Project implementation defaults to 2.25 seconds, but live demo polling every 750ms.
    * Variety
      * Many different eventy types, e.g., for pushing commits, opening/closing/merging pull request, starring repositories, creating forks, opening/closing issues, etc.
      * Varied nested JSON structures representing the various event types.
    * Delay
      * The events retrieved from the API are about 5 minutes behind real-time.
      * They are generally in order of created time with some exceptions.
      * 95% of events are in-order.
      * 99.8% of events are less than 10 seconds out of order.
      * 99.9% of events are no more than 5 minutes out of order.
      * However, some events are significantly delayed, multiple days even.
* Technologies and overall architecture
  * technologies (languages, libraries/frameworks, ...; a few sentences each)
    * Backend components all implemented in Java. 
      * Using RxJava for stream processing.
      * Argument parsing using Argparse4j.
      * Using Jackson for JSON parsing.
      * Logging using SLF4J and Logback for logging.
      * Using JUnit for testing.
    * Frontend client implemented in as a single page application.
      * Written in TypeScript.
      * Vite used to build the project.
      * Using React, React Router, and Tailwind CSS for the UI.
      * Using Lucide icons and Recharts for the charts.
      * RxJS used mainly for the WebSocket client.
    * Apache Kafka
    * Apache Flink
    * PostgreSQL with TimescaleDB
    * Docker and Docker Compose
  * system architecture with required diagram
    * Ingestion
      * Java based custom Kafka Producer.
      * Polls the GitHub Events API.
      * Publishes events to a Kafka topic.
    * Messaging
      * Kafka is used for real-time event streaming.
      * One topic for the raw events.
      * Multiple topics for the outputs of the processor.
    * Storage
      * PostgreSQL + TimescaleDB used to store the results of the processor,
      * Enables clients to get an initial snapshot for populating the dashboard.
      * Allows for retrieving historical data.
    * Processing
      * Data processing is performed by a Flink job.
      * Implemented in Java.
      * Raw events are read from Kafka.
      * Results are written both to Kafka topics and tables in the PostgreSQL database.
    * Frontend
      * Server
        * Provides an HTTP server that serves static files.
        * Provides an API over WebSockets.
        * Reads snapshots from PostgreSQL.
        * Reads real-time updates from Kafka.
      * Client
        * Implemented as a single page application.
        * Communicates with the server over WebSocket based API.
  * workflow (i.e., data processing steps + any manual actions involved in using the system)
    * Flink Processing Pipeline
      * Read raw events from Kafka.
      * Deserialize JSON, assign timestamps, and generate watermarks.
      * Extract fields and generate human-readable description.
      * Extract user and repository details.
      * Perform different windowed aggregations. Per-user, per-repo, and globally.
        * Tumbling windows with 10s and 5m window size. Used for charts.
        * Sliding windows with 5m, 1h, 6h, 24h window updated every second.
      * Generate rankings based on the computed window aggregates.
      * Write to Kafka + DB.
  * Scalability and Reliability
    * Increased Kafka partitions and Flink parallelism.
      * Some task, like ranking, are still inherently not parallelize well.
    * Kafka message retention ensures restart of Flink job is possible without loosing events.
    * Flink setup for high availability with checkpoints and zookeeper.
    * Event-time processing is used in Flink.
      * Timestamps directly taken from the `created_at` field in the GitHub events.
      * Watermarks are assigned with maximum 10s out-of-orderness.
      * Wherever appropriate, late events are still counted.
        * For live sliding window counts, they are counter if the window is still active.
        * For 5m tumbling windows allowed lateness is set to 50m. For 10s windows to 100s.
    * If producer goes down, events will be lost. Could be solved by running multiple producers, since there is a deduplication step in the Flink processor.
* Functionalities
  * including screenshots for illustration
    * Live Dashboard
      * Live Event Stream
        * Displays latest events.
        * Human-readable description of the events.
        * Filters by: Kind, User, Repository.
      * Sliding Window Counters
        * Updated every second, aggregate over last 5m, 1h, 6h, and 24h.
        * Counts for each kind of event.
      * Historical Counts
        * Shows a chart of event counts over time.
      * Leaderboards
        * Most active users.
        * Most active repositories.
        * Repositories with the highest number of new stars.
        * Repositories with the highest trending score.
      * Trending Detection
        * Trending score based on linear combination of stars in the last 5m, 1h, 6h, and 24h.
    * Event Kind Pages
      * Insights into specific event types.
      * Shows historical counts for specific event type.
      * Shows distribution of event types in the last hour (pie chart) and over time (stacked area chart).
    * Repository Pages
      * Per repository statistics: number of events and number of stars in sliding window.
      * Ranking of the repository based on the above metrics.
      * Historical charts of event counts and new stars.
    * User Pages
      * Per user statistics based on number of events.
      * Ranking of the user based on the number of events.
      * Historical charts of event counts.
    * Search Bar
      * Allows searching for specific users and repositories.
      * Brings one to the dedicated repository or user page.
* Lessons learned
  * e.g., what have worked? what not? what would you improve?
  * Getting the high frequency updates (1s) for long windows (24h) required implementing a custom KeyedProcessFunction, because regular Flink windowing was too inefficient.
  * Getting the complete ranking update in real-time was challenging. Required cooperation from Flink, PostgreSQL, and the client.
  * GitHub changed the API around October, leading to it including less information.
  * Debugging the system, especially the Flink processor, was a significant challenge due to limited visibility into the state of a Flink operator.
  * Improvements:
    * More sophisticated tending detection. Current approach is very simplistic.
    * More data sources, e.g., tracking mentions of repositories on social media to better understand trending behavior.
    * Analysis of commit, issue, and pull request comment messages.
    * More visualizations of the existing data.
