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
    * Is the primary datasource
    * `/events` HTTP endpoint
    * Retrieving data using periodic polling
    * Authentication via tokens
  * Dummy API and GHArchive
    * Usage of dummy API for testing and offline development
    * Does not require an API token
    * Uses historical GHArchive data
  * Data Characteristics
    * Volume and Velocity
      * Thousands of events per minute: Generally hovering around 70-120 events/second depending on the time of day
      * Results in millions of events per day.
      * 170-200MiB per hour
      * Can be polled at different intervals. Project implementation defaults to 2.25 seconds, but live demo polling every 750ms.
    * Variety
      * Many different eventy types, e.g., for pushing commits, opening/closing/merging pull request, starring repositories, creating forks, opening/closing issues, etc.
    * Delay
      * 99.8% of events are less than 10 seconds out of order.
      * 99.9% of events are no more than 5 minutes out of order.
      * However, some events are significantly delayed, multiple days even
* Technologies and overall architecture
  * technologies (languages, libraries/frameworks, ...; a few sentences each)
  * system architecture with required diagram
  * workflow (i.e., data processing steps + any manual actions involved in using the system)
* Functionalities
  * including screenshots for illustration
* Lessons learned
  * e.g., what have worked? what not? what would you improve?
