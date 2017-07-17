package com.codenotfound.kafka.producer;

import com.codenotfound.kafka.AllSpringKafkaTests;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.kafka.test.assertj.KafkaConditions.value;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringKafkaSenderTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringKafkaSenderTest.class);


    private KafkaMessageListenerContainer<String, String> container;

    private BlockingQueue<ConsumerRecord<String, String>> records;

    @Autowired
    private Sender sender;

    @Before
    public void setUp() throws Exception {
        // set up the Kafka consumer properties
        Map<String, Object> consumerProperties =
                KafkaTestUtils.consumerProps("sender_group", "false", AllSpringKafkaTests.embeddedKafka);

        // create a Kafka consumer factory
        DefaultKafkaConsumerFactory<String, String> consumerFactory =
                new DefaultKafkaConsumerFactory<String, String>(consumerProperties);

        // set the topic that needs to be consumed
        ContainerProperties containerProperties =
                new ContainerProperties(AllSpringKafkaTests.SENDER_TOPIC);

        // create a Kafka MessageListenerContainer
        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

        // create a thread safe queue to store the received message
        records = new LinkedBlockingQueue<>();

        // setup a Kafka message listener
        container.setupMessageListener(new MessageListener<String, String>() {
            @Override
            public void onMessage(ConsumerRecord<String, String> record) {
                LOGGER.debug("test-listener received message='{}'", record.toString());
                records.add(record);
            }
        });

        // start the container and underlying message listener
        container.start();
        // wait until the container has the required number of assigned partitions
        ContainerTestUtils.waitForAssignment(container,
                AllSpringKafkaTests.embeddedKafka.getPartitionsPerTopic());
    }

    @After
    public void tearDown() {
        // stop the container
        container.stop();
    }

    @Test
    public void testSendBlocking() throws Exception {
        // send the message
        String content = "User viewed page A";
        Integer userId = 12;
        sender.sendBlocking(AllSpringKafkaTests.SENDER_TOPIC, content, userId);

        // check that the message was received
        assertThat(records.poll(10, TimeUnit.SECONDS)).has(value(content));
    }

    @Test
    public void testSendAsync() throws Exception {
        // send the message
        String content = "User viewed page A";
        Integer userId = 123;
        sender.sendAsync(AllSpringKafkaTests.SENDER_TOPIC, content, userId);

        // check that the message was received
        assertThat(records.poll(10, TimeUnit.SECONDS)).has(value(content));
    }


}
