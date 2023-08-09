package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Consumer Demo!");

        String groupId = "my-java-application";
        String topic = "demo_java";

        Properties properties = new Properties();
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"gQmsfDYIE3UsFWqrEJLiT\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJnUW1zZkRZSUUzVXNGV3FyRUpMaVQiLCJvcmdhbml6YXRpb25JZCI6NzUzNTUsInVzZXJJZCI6ODc2NzcsImZvckV4cGlyYXRpb25DaGVjayI6IjhlYzNmMjY5LTMwOTItNDJmNC1hYmQ4LTk0MmE5ODg5M2VjMSJ9fQ.BMbouxvQl1nIEVnyE52CSUT5DwV9eW-ahfKwpals1XU\";\n");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());

        properties.setProperty("group.id",groupId);
        properties.setProperty("auto.offset.reset","earliest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Shutdown detected!");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });


        try {
            consumer.subscribe(Arrays.asList(topic));

            while (true) {
                log.info("Polling");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + "Value: " + record.value());
                    log.info("Partition: " + record.partition() + "OffSet: " + record.offset());
                }
            }
        }
        catch(WakeupException e){
            log.info("Consumer is starting to shutdown");
        }
        catch(Exception e){
            log.info("Unexpected Exception");
        } finally {
            consumer.close();
            log.info("Consumer is now shutdown gracefully");
        }
    }

}
