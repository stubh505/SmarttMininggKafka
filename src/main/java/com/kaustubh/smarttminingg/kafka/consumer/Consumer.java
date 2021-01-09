package com.kaustubh.smarttminingg.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    private final String topic;
    private final File dataSet;
    private final Logger logger = LoggerFactory.getLogger(Consumer.class);

    public Consumer(String topic, File dataSet) {
        this.topic = topic;
        this.dataSet = dataSet;
    }

    public KafkaConsumer<String, String> createConsumer() {

        String bootstrapServer = "127.0.0.1:9092";

        //creating properties
        logger.info("Creating Properties");
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // creating consumer
        logger.info("Properties Set");
        return new KafkaConsumer<>(properties);
    }

    public boolean consume() throws IOException {
        logger.info("Creating Consumer");
        KafkaConsumer<String, String> consumer = createConsumer();
        logger.info("Consumer Created");

        consumer.subscribe(Collections.singleton(topic + "_FILTERED"));

        BufferedWriter br = new BufferedWriter(new FileWriter(dataSet));

        int retry = 5;
        // poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            //Try for 5 times, If no messages then exit.
            if (records.count()==0) {
                retry--;
                if (retry == 0)
                    break;
                else
                    continue;
            }

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key : "+record.key()+" Value : "+record.value());
                logger.info("Partition : "+record.partition()+ " Offset : "+record.offset());
                br.write(record.value());
            }
        }

        br.close();
        consumer.close();
        return true;
    }
}
