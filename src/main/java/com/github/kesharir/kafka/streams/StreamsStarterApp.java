package com.github.kesharir.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        // 1 - stream from Kafka
        KStream<String, String> wordCountInput = builder.stream("wordcount-input");
        // 2 - map values to lowercase
        KTable<String, Long> wordCounts = wordCountInput.mapValues(value -> value.toLowerCase())
        // 3 - flatmap values split by space
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
        // 4 - select key to apply a key (we discard the old key)
                .selectKey((ignoredKey, word) -> word)
        // 5 - group by key before aggregation
                .groupByKey()
        // 6 - count occurences
                .count();
        wordCounts.toStream().to("word-count-output");
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
        // printed the topology:
        System.out.println(streams.toString());
        // Add shutdown hook to stop the Kafka Streams threads.
        // You can optionally provide a timeout to `close`
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
