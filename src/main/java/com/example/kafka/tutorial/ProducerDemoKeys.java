package com.example.kafka.tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        String topic = "first_topic";



        for(int i=0;i<10;i++) {

            String value = "hello World "+ Integer.toString(i);

            String key = "id_"+Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key,value);

            logger.info("Key: "+key);

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new Metadata" + "\n" +
                                "Topic " + recordMetadata.topic() + "\n" +
                                "Partition " + recordMetadata.partition());
                    } else {
                        logger.error("Error", e);
                    }
                }
            }).get();


        }
        producer.flush();
        producer.close();
    }
}
