# Development

This document describes how to set up all the components to run outside of docker. This is useful for a development environment, since it allows for faster restarts of the components.

## Prerequisites

To set up and run this project outside of docker, you will need to run a couple of components yourself (our by using a subset of `COMPOSE_PROFILES` in the docker compose setup):
* A Kafka cluster with at least one broker.
* A PostgreSQL database with the TimescaleDB extension installed.
* A Flink cluster with a job manager to submit jobs to (optional).

You will also have to have the following dependencies installed:
* Java 17
* Maven
* Node.js
* npm

## Running the Application Components

Here is a description of how to run all the components of the project outside docker. All the backend components are based on Java and using Maven. You can build all of them using `mvn package` and to run only their associated test cases using `mvn test`.

### Dummy GitHub Events API Server

The dummy API server must only be run if you want to not use the official GitHub API. This may be useful if you don't want to generate a GitHub API key. To run it, navigate to `app/ghdummy` and run:
```bash
mvn package -DskipTests
java -jar target/ghdummy-0.1.0.jar
```
The default configuration should work well, but you may provide any of the following command line arguments to modify the behavior:
* `--port=8889` to set the port on which the server listens.
* `--speed-up=1.0` the control how much faster than real time the dummy server will serve events.
* `--data-dir=.cache` will set a directory used to cache the events. If not provided, they will not be cached on disk.
* `--archive-url=https://data.gharchive.org` the URL from which to get the events. By default, events are loaded from GHArchive, a service that provide the events without needing an API token.
* `--archive-delay=24` the initial delay, in hours, between the real time and the served events. Note that GHArchive releases are generally delayed by 2-3 hours.
* `--log-level=debug` the log level of the application.

### Producer

To run the producer, make sure the Kafka broker is running and navigate to the `app/producer` directory and run:
```bash
mvn package -DskipTests
java -jar target/producer-0.1.0.jar
```
The default configuration should work together with the default configuration for the other components in the project. You may optionally provide any of the following command line arguments to modify the behavior:
* `--bootstrap-servers=localhost:29092` to set the address of the Kafka broker to connect to.
* `--topic=raw_events` the Kafka topic to publish the raw events to.
* `--num-partitions=4`, `--replication-factor=1`, and `--retention-ms=604800000` to configure respectively the number of partitions, replication factor, and retention time of the Kafka topic.
* `--url=http://localhost:8889` to configure the URL of the Events API to poll. For using the dummy API server, use `http://localhost:8889` and to use the real GitHub Events API use `https://api.github.com`.
* `--gh-token=github_token` Replace `github_token` with your GitHub API token if you are not using the dummy Events API server. This may also be a comma separated list of multiple tokens, in which case round-robin load balancing will be used among them.
* `--poll-ms=2250` and `--poll-depth=300` to configure the polling behavior of the producer. Respectively, these set the polling interval in milliseconds and the number of events to retrieve for each poll.
* `--log-level=debug` the log level of the application.
* `--dry-run` to not publish to Kafka, instead printing to standard output.

### Processor

The processor is a Flink job. You can submit it to the Flink cluster using the `flink` command-line tool. Alternatively, you can run the processor as a standalone Java application. This is useful for debugging. In either case, first you must navigate to `app/processor` and package the processor application by running:
```bash
cd app/processor
mvn package
```

#### Running on a Flink Mini-cluster

To run it as a standalone application, you may use the provided `run.sh` script. It contains the necessary options and class path adjustments necessary because the generated package does not actually include the Flink dependencies. You may run the following:
```bash
bash run.sh
```
All command line arguments provided to the script will be passed on to the application. You may use the following options to modify the default behavior:
* `--bootstrap-servers=localhost:29092` to set the address of the Kafka broker to connect to.
* `--topic=raw_events` the Kafka topic to read the raw events from.
* `--group-id=processor` the group ID to use for the Kafka consumer.
* `--db-url=jdbc:postgresql://localhost:25432/db` the JDBC connection URL of the database.
* `--db-username=user` and `--db-password=user` to configure username and password for connecting to the database.
* `--parallelism=1` the parallelism with which to configure the Flink job.
* `--ui-port=8081` tun the Flink UI at the specified port when running standalone.
* `--num-partitions=4`, `--replication-factor=1`, and `--retention-ms=900000` to configure respectively the number of partitions, replication factor, and retention time of the created Kafka topics.
* `--rewind` if set, the processor will restart at the beginning of the Kafka topic, instead of starting at the committed offset.
* `--dry-run` to not publish to Kafka, instead printing all output to standard output.

#### Submitting to a Flink Cluster

Alternatively, to submit the Job to the Flink cluster run the following inside a Flink container:
```bash
flink run -c com.rolandb.Processor /app/processor.jar \
    --bootstrap-servers broker:9092 \
    --db-url jdbc:postgresql://postgres:5432/db
```
The same options as when running as a standalone application may be used also here. See the instructions above for details.

### Frontend

The frontend is split into two parts. The first, written in Java and directly located in `app/frontend`, is the server component, providing both a HTPP and a WebSocket interface for the client to interface with. The second, is the client part, which is implemented as a single page web application and located in `app/frontend/web`.

#### Client

The frontend is a React application using Vite. To build the application into a statically hostable set of files, necessary for use with the frontend server of this project, run the following commands:
```bash
npm install
npm run build
```
The resulting files will be found in `app/frontend/web/dist` after the build finishes.

Alternatively, to run it in development mode, navigate to the `app/frontend/web` directory and run:
```bash
npm install
npm run dev
```
The application will be available at `http://localhost:5173`. The main server will still need to run to provide the WebSocket based API, so follow also the instructions below. The tests for the component can be run with `npm run test`.

#### Server

To run the frontend server, first you must build the frontend following the instructions above. Then, navigate to the `app/frontend` directory and run:
```bash
mvn package -DskipTests
java -jar target/frontend-0.1.0.jar
```
The server will be available at `http://localhost:8888`. You may optionally provide any of the following command line arguments to modify the behavior:
* `--port=8888` to set the port on which the HTTP server listens for connections.
* `--ws-port=8887` to set the port used for the WebSocket API.
* `--bootstrap-servers=localhost:29092` to set the address of the Kafka broker to connect to.
* `--group-id=frontend` the group ID to use for the Kafka consumer.
* `--db-url=jdbc:postgresql://localhost:25432/db` the JDBC connection URL of the database.
* `--db-username=user` and `--db-password=user` to configure username and password for connecting to the database.
* `--static-dir=web/dist` to configure the directory from which to serve static files. Make sure it contains the result of building the client part of the frontend.
* `--log-level=debug` the log level of the application.
* `--secret` to configure a password required to access the frontend.

