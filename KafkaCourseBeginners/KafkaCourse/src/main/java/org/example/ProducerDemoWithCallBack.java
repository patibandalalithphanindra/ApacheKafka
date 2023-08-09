package org.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallBack.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka with callback!");

        Properties properties = new Properties();
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"gQmsfDYIE3UsFWqrEJLiT\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJnUW1zZkRZSUUzVXNGV3FyRUpMaVQiLCJvcmdhbml6YXRpb25JZCI6NzUzNTUsInVzZXJJZCI6ODc2NzcsImZvckV4cGlyYXRpb25DaGVjayI6IjhlYzNmMjY5LTMwOTItNDJmNC1hYmQ4LTk0MmE5ODg5M2VjMSJ9fQ.BMbouxvQl1nIEVnyE52CSUT5DwV9eW-ahfKwpals1XU\";\n");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty("batch.size","400");

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        for(int j=0;j<10;j++){
            for(int i=0;i<10;i++){
                ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java","hello world" + i);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if(e == null){
                            log.info("Successful Completion!");
                            log.info(metadata.topic() + "\n"+ metadata.partition() + "\n" + metadata.offset() + "\n" +
                                    metadata.timestamp() );
                        }
                        else{
                            log.error("Error while producing ", e);
                        }

                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }



        producer.flush();

        producer.close();

    }

}
