package com.kaustubh.smarttminingg.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Scanner;

public class Producer {

    private final String topic;
    private final File dataSet;
    private final Logger logger = LoggerFactory.getLogger(Producer.class);

    public Producer(String topic, File dataSet) {
        this.topic = topic;
        this.dataSet = dataSet;
    }

    public KafkaProducer<String, String> createProducer() {

        String bootstrapServer = "127.0.0.1:9092";

        //creating properties
        logger.info("Creating Properties");
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // creating producer
        logger.info("Properties Set");
        return new KafkaProducer<>(properties);
    }

    public boolean produceMessages() throws FileNotFoundException {
        logger.info("Creating Producer");
        KafkaProducer<String, String> producer = createProducer();
        logger.info("Producer created");

        // reading data from file
        logger.info("Reading messages");
        Scanner sc = new Scanner(dataSet);

        while (sc.hasNext()) {
            String data = sc.nextLine();

            String[] cells = data.split(",");

            ProducerRecord<String, String> record = new ProducerRecord<>(this.topic, cells[0], data);

            producer.send(record, ((recordMetadata, e) -> {
                if (e == null) {
                    logger.info("Message received at : Offset : " + recordMetadata.offset()
                            + " Partition : " + recordMetadata.partition());
                } else {
                    logger.error("Error occurred", e);
                }
            }));
        }

        logger.info("All messages produced");
        producer.close();
        logger.info("Producer closed");

        return true;
    }
}
