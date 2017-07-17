package com.codenotfound.kafka.consumer;

import com.codenotfound.kafka.AllSpringKafkaTests;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.codenotfound.kafka.AllSpringKafkaTests.CONSUMER_TEST_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringKafkaReceiverTest {
    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    private Producer<Integer, String> kafkaProducer;

    private KafkaConsumerWrapper kafkaConsumer;


    @Before
    public void setUp() throws Exception {
        // set up the Kafka producer properties
        Map<String, Object> senderProperties =
                KafkaTestUtils.senderProps(AllSpringKafkaTests.embeddedKafka.getBrokersAsString());

        // create a Kafka producer factory
        ProducerFactory<Integer, String> producerFactory =
                new DefaultKafkaProducerFactory<>(senderProperties);

        kafkaProducer = producerFactory.createProducer();

        // wait until the partitions are assigned
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
                .getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer,
                    AllSpringKafkaTests.embeddedKafka.getPartitionsPerTopic());
        }

        kafkaConsumer = new KafkaConsumerWrapper(
                KafkaTestUtils.consumerProps("group_id", "false", AllSpringKafkaTests.embeddedKafka),
                CONSUMER_TEST_TOPIC
        );
    }

    @Test
    public void givenConsumer_whenSendMessageToIt_thenShouldReceiveInThePoolLoop() throws Exception {
        //given
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        String message = "Send unique message " + UUID.randomUUID().toString();

        //when
        executorService.submit(() -> kafkaConsumer.startConsuming());

        kafkaProducer.send(new ProducerRecord<>(CONSUMER_TEST_TOPIC, message)).get(1, TimeUnit.SECONDS);
        kafkaProducer.send(new ProducerRecord<>(CONSUMER_TEST_TOPIC, message)).get(1, TimeUnit.SECONDS);
        kafkaProducer.send(new ProducerRecord<>(CONSUMER_TEST_TOPIC, message)).get(1, TimeUnit.SECONDS);
        kafkaProducer.send(new ProducerRecord<>(CONSUMER_TEST_TOPIC, message)).get(1, TimeUnit.SECONDS);

        //then
        executorService.awaitTermination(4, TimeUnit.SECONDS);
        executorService.shutdown();
        assertThat(kafkaConsumer.consumedMessages.get(0).value()).isEqualTo(message);
    }

}
