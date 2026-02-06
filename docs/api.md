# WebSocket API

The WebSocket API provides a real-time stream of data from the server to the client.

## Connection

The WebSocket API is available on port 8887 on the same host as the web application. For example, if the application is running at `http://localhost:8888`, the WebSocket API will be at `ws://localhost:8887`.

## Client-to-Server Messages

### subscribe

Subscribes to a data stream. After a successful subscription request, the server will send real-time updates whenever a new message is received from the corresponding Kafka topic.
```json
{
  "subscribe": [
    {
      "id": 0,
      "table": "events",
      "filters": [
        {
          "kind": { "opt": ["PushEvent"] }
        }
      ]
    }
  ]
}
```
* `id`: A unique ID for the subscription. Used to cancel the subscription.
* `table`: The name of the topic to subscribe to.
* `filters`: An array of filters to apply to the data.

### replay

Replays all rows that are currently part of the table in PostgreSQL and matching the filters. This is intended for getting the initial state of the dashboard, without having to rewind the complete Kafka topic.
```json
{
  "replay": [
    {
      "id": 0,
      "table": "events",
      "filters": [
        {
          "kind": { "opt": ["PushEvent"] }
        }
      ],
      "limit": 100
    }
  ]
}
```
* `id`: A unique ID for the replay. This is used in the response messages to identify the replay.
* `table`: The name of the table to subscribe to.
* `filters`: An array of filters to apply to the data.
* `limit`: The maximum number of rows to return. Note that for some tables this might be required, to avoid sending too much data.

### unsubscribe

Unsubscribes from a data stream.
```json
{
  "unsubscribe": [0]
}
```
* An array of subscription IDs. Will cancel the subscriptions that have been created with the given IDs.

## Server-to-Client Messages

### row

A message containing a single row of data.
```json
{
  "table": "events",
  "row": {
    "seq_num": 0,
    "id": "12345",
    "type": "PushEvent",
    "repo": "rolandbernard/rtbdp-project",
    "user": "rolandbernard",
    "created_at": "2026-01-10T12:00:00Z"
  }
}
```
* `table`: The name of the table the row belongs to.
* `row`: The row data.

### rows

A message containing multiple rows of data at once.
```json
{
  "table": "events",
  "rows": {
    "seq_num": [0, 1],
    "id": ["12345", "67890"],
    "type": ["PushEvent", "PullRequestEvent"],
    "repo": ["rolandbernard/rtbdp-project", "rolandbernard/rtbdp-project"],
    "user": ["rolandbernard", "rolandbernard"],
    "created_at": ["2026-01-10T12:00:00Z", "2026-01-10T12:01:00Z"]
  }
}
```
* `table`: The name of the table the rows belong to.
* `rows`: The row data, organized by column. This reduces the JSON serialization overhead and leads to more efficient transmission.

### replayed

Indicates that a replay has completed. This is sent after all rows from a replay request have been sent to the client using a combination of the above two formats.
```json
{
  "replayed": 100
}
```
