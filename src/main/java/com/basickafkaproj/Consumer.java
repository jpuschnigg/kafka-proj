package com.basickafkaproj;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import java.io.FileWriter;
import java.io.IOException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {

    public static void main(String[] args) {

        // Create Logger for Class
        final Logger logger = LoggerFactory.getLogger(Consumer.class);

        // Create Variables for Strings
        final String bootstrapservers = "127.0.0.1:9092";
        final String consumerGroupID = "basic-kafka-proj-consumer";
        String writeInfo = "";

        // Create and Populate Properties Object
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupID);
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Consumer
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(p);

        // Subscribe to Topic
        consumer.subscribe(Arrays.asList("basic-kafka-proj"));

        // Poll and Consume Records
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("\nReceived new record: \n" +
                        "Key: " + record.key() + ", " +
                        "Value: " + record.value() + ", " +
                        "Topic: " + record.topic() + ", " +
                        "Partition: " + record.partition() + ", " +
                        "Offset: " + record.offset() + "\n");

                writeInfo +=
                        "Key: " + record.key() + ", " +
                        "Value: " + record.value() + ", " +
                        "Topic: " + record.topic() + ", " +
                        "Partition: " + record.partition() + ", " +
                        "Offset: " + record.offset() + "\n";
            }

            try {
                // Create FileWriter
                FileWriter fWrite = new FileWriter("src/main/resources/records.txt");
                fWrite.write(writeInfo);
                fWrite.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            consumer.close();
        }
    }
}
