package com.himanshu.kafkastream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public class wordCount {

    public static void main(String[] args) {
        Properties prop= new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG,"word-count-app");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        prop.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.<String,String>stream("sentences").
                flatMapValues((key ,value) -> Arrays.asList(value.toLowerCase().split(" "))).
                groupBy((key, value) -> value).
                count(Materialized.with(Serdes.String(),Serdes.Long()))
                .toStream().to("word-count", Produced.with(Serdes.String(),Serdes.Long()));

        KafkaStreams kafkaStreams= new KafkaStreams(streamsBuilder.build(),prop);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}
