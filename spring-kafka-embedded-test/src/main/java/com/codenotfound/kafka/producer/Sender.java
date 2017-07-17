package com.codenotfound.kafka.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.TimeUnit;

public class Sender {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private Producer<String, String> producer;

    public void sendBlocking(String topic, String data, String partitionKey) {
        LOGGER.info("sending data='{}' to topic='{}'", data, topic);
        try {
            producer.send(new ProducerRecord<>(topic, null, data)).get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendAsync(String topic, String data, String partitionKey) {
        LOGGER.info("sending data='{}' to topic='{}'", data, topic);
        producer.send(new ProducerRecord<>(topic, null, data), new AsyncSenderCallback());
    }
}
