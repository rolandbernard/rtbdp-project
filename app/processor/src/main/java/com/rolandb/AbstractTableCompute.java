package com.rolandb;

/**
 * This class contains the basic logic for writing a computed table to both a
 * PostgreSQL table and simultaneously the change log also to a separate Kafka
 * topic. The goal is to make it possible for a client to connect to the Kafka
 * topic, fetch the latest state from the database and then update it based on
 * the messages from the Kafka topic.
 */
public class AbstractTableCompute {
    
}
