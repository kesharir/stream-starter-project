package com.github.kesharir.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class FavouriteColourApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        Set<String> FAVOURITE_COLOURS = new HashSet<String>() {{
            add("RED");
            add("BLUE");
            add("GREEN");
        }};
        StreamsBuilder builder = new StreamsBuilder();
        // 1 - stream from Kafka
        KStream<String, String> favouriteColoursInputStream = builder.stream("favorite-colour-input");
        // Filtering out the data to a intermediate topic:
        // &
        //
        KStream<String, String> favouriteColourIntermediateStream =
                favouriteColoursInputStream.filter((key, value) -> value.chars().filter(ch -> ch == ',').count() == 1)
                        .selectKey((key, value) -> value.split(",")[0])
                        .mapValues( value -> value.split(",")[1].toUpperCase())
                        .filter((key, value) -> FAVOURITE_COLOURS.contains(value));
            favouriteColourIntermediateStream.to("favorite-colour-intermediate");
        // Reading data from a intermediate topic as KTable
        KTable<String, Long> favouriteColoursTable = builder.table("favorite-colour-intermediate")
                .groupBy((user, colour) -> new KeyValue<>((String)colour, colour))
                .count();
        // Write to kafka final topic: favorite-colour-output
        favouriteColoursTable.toStream().to("favorite-colour-output", Produced.with(Serdes.String(), Serdes.Long()));
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
        // printed the topology:
        System.out.println(streams.toString());
        // Add shutdown hook to stop the Kafka Streams threads.
        // You can optionally provide a timeout to `close`
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
