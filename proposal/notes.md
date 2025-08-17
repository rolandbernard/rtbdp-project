Proposal of ~300 words.
* Title: Real-Time Analysis of Public GitHub Activity
* Application Domain
    * Software development platform.
    * Monitor and analyse public GitHub events.
    * Events include:
        * Creating new repositories, branches, or forks.
        * Pushing commits to public repositories.
        * Opening or closing of issues and pull requests.
        * Starring of repositories.
* Data Sources
    * GitHub Events API.
    * The api.github.com/events endpoint serves the most recent public events for the complete platform.
    * Events are served as JSON objects containing details about the action.
    * By polling this endpoint we can get a stream of events.
* Technologies and Architecture
    * Data Ingestion: A custom Java Kafka Producer will periodically poll the GitHub Events API, de-duplicate the JSON events, and publish them to a Kafka topic.
    * Stream Processing: Apache Flink will consume the raw event stream from Kafka. It will perform various stateful computations, aggregations, and transformations to derive meaningful analytics.
    * Data Serving: The processed, enriched data from Flink will be written to new Kafka topics. A Java-based backend server will consume these topics and serve the results to the frontend.
    * Frontend: A custom HTML and JavaScript single-page application will visualize the real-time analytics. ECharts will be used to create interactive and dynamic charts and graphs.
* Functionalities
    * 
