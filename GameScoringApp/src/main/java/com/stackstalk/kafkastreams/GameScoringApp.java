package com.stackstalk.kafkastreams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

public class GameScoringApp {

	public static void main(String[] args) {
		
		// Configuration for a Kafka Streams instance
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "game-scoring");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		// Create a KStream to read  the specific topic
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> scoresInput = builder.stream("scores-input-1");
		
		// Do an aggregation on the KStream to get KTable
		// Here we group by key and add the new value
		// Aggregate the values of records in this stream by the grouped key. 
		// Records with null key or value are ignored. 
		// The result is written into a local KeyValueStore 
		KTable<String, Long> scoresOutput = scoresInput
				.groupByKey()
				.aggregate(() -> 0L, (key, value, total) -> total + Long.parseLong(value),
				Materialized.with(Serdes.String(), Serdes.Long()));
		
		// Materialize the stream to a output topic
		scoresOutput.toStream()
		.peek((key, value) -> System.out.println("Key:Value = " + key + ":" + value))
		.to("scores-output");
		
		// Print the topology
		Topology topology = builder.build();
		System.out.println(topology.describe());
		
		// Create the Kafka Streams instance
		KafkaStreams kafkaStreams = new KafkaStreams(topology, config);
		
		// Clean local state prior to starting the processing topology
		// In production we need to cleanup only on certain conditions
		kafkaStreams.cleanUp();
		
		// Start the KafkaStreams instance
		kafkaStreams.start();

		// Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
	}
}
