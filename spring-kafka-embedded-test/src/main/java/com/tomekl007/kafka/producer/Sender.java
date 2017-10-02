package com.tomekl007.kafka.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Sender {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

    @Autowired
    private Producer<Integer, String> producer;

    public RecordMetadata sendBlocking(String topic, String data, Integer partitionKey) {
        LOGGER.info("sending data='{}' to topic='{}'", data, topic);
        try {
            throw new NotImplementedException();
        } catch (Exception e) {
            throw new RuntimeException("problem when send", e);
        }finally {
            producer.flush();
        }
    }

    public Future<RecordMetadata> sendAsync(String topic, String data, Integer partitionKey) {
        LOGGER.info("sending data='{}' to topic='{}'", data, topic);
        try {
            throw new NotImplementedException();
        }finally {
            producer.flush();
        }
    }
}
