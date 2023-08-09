package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Producer Demo!");

        Properties properties = new Properties();
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"gQmsfDYIE3UsFWqrEJLiT\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJnUW1zZkRZSUUzVXNGV3FyRUpMaVQiLCJvcmdhbml6YXRpb25JZCI6NzUzNTUsInVzZXJJZCI6ODc2NzcsImZvckV4cGlyYXRpb25DaGVjayI6IjhlYzNmMjY5LTMwOTItNDJmNC1hYmQ4LTk0MmE5ODg5M2VjMSJ9fQ.BMbouxvQl1nIEVnyE52CSUT5DwV9eW-ahfKwpals1XU\";\n");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java","hello world");

        producer.send(producerRecord);

        producer.flush();

        producer.close();

    }

}
