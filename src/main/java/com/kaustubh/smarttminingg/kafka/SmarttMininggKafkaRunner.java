package com.kaustubh.smarttminingg.kafka;

import com.kaustubh.smarttminingg.kafka.consumer.Consumer;
import com.kaustubh.smarttminingg.kafka.filter.Filter;
import com.kaustubh.smarttminingg.kafka.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class SmarttMininggKafkaRunner {

    private static Logger logger = LoggerFactory.getLogger(SmarttMininggKafkaRunner.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        String topic = args[0];
        String dataset = args[1];
        String output = args[2];

        logger.info("Starting SmarttMininggKafkaRunner for");
        logger.info("Topic : "+topic+", Dataset : "+dataset);

        boolean res = new Producer(topic, new File(dataset)).produceMessages();

        if (res) {
            logger.info("Production complete");
        }

        for (int i = 10; i >= 0; i--) {
            logger.info("Sleeping for "+i+" seconds");
            Thread.sleep(1000);
        }

        logger.info("Starting filter");
        new Filter().stream(topic);
        logger.info("Filtering complete");

        for (int i = 10; i >= 0; i--) {
            logger.info("Sleeping for "+i+" seconds");
            Thread.sleep(1000);
        }

        res = new Consumer(topic+"_FILTERED", new File(output)).consume();

        if (res) {
            logger.info("Consumption complete");
        }

        logger.info("Exiting Process");
    }
}
