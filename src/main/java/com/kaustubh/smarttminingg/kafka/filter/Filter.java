package com.kaustubh.smarttminingg.kafka.filter;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Filter {

    Logger logger = LoggerFactory.getLogger(Filter.class);
    public void stream(String topic) {
        //Create configuration object with all parameters set.
        logger.info("Setting Properties");
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "ClickStreamAnalysis");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //Create a stream builder
        StreamsBuilder clickStreamBuilder = new StreamsBuilder();

        //Stream the messages from the specified Topic
        KStream<String, String> StreamData = clickStreamBuilder.stream(topic);

        //Perform filter operation on SessionTime parameter
        //Filter the records where SessionTime is less than 30 seconds
        logger.info("Filtering messages");
        KStream<String, String> FilteredRecords = StreamData.filter(

                (Key, Value) -> {

                    String[] splitValues = Value.split(",");

                    int tyrePressure = Integer.parseInt(splitValues[3]);
                    int engine = Integer.parseInt(splitValues[2]);
                    String weather = splitValues[4];

                    boolean fail = (tyrePressure > 50) || (engine < 100) || (weather.equalsIgnoreCase("snow")) || weather.equalsIgnoreCase("heavyRain");
                    return fail;
                }

        );

        //Write filtered records to a new topic
        FilteredRecords.to(topic+"_FILTERED");

        logger.info("Cleansing the records is complete");

        //Create KafkaStreams object and start the process
        KafkaStreams streams = new KafkaStreams(clickStreamBuilder.build(),properties);

        streams.start();
    }
}
