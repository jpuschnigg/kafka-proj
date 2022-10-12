package com.basickafkaproj;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {
    public static void main(String[] args) {

        // Create Logger
        final Logger logger = LoggerFactory.getLogger(Producer.class);

        // Create properties object for Producer
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer
        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        for (int i = 1; i <= 10; i++) {
            // Create the ProducerRecord
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("basic-kafka-proj", "key_" + i,
                    "value_" + i);

            // Send Data - Asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("\nReceived record metadata. \n" +
                                "Topic: " + recordMetadata.topic() + ", Partition: " + recordMetadata.partition() + ", "
                                +
                                "Offset: " + recordMetadata.offset() + " @ Timestamp: " + recordMetadata.timestamp()
                                + "\n");
                    } else {
                        logger.error("Error Occured", e);
                    }
                }
            });
        }

        // Flush and close the Producer
        producer.flush();
        producer.close();
    }
}
