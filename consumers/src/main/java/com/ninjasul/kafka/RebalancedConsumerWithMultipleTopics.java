package com.ninjasul.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class RebalancedConsumerWithMultipleTopics {
    public static final Logger log = LoggerFactory.getLogger(RebalancedConsumerWithMultipleTopics.class);

    public static void main(String[] args) {
        Properties props = buildProperties();

        KafkaConsumer<String, String> kafkaConsumer = buildConsumer(props, List.of("topic-p3-t1", "topic-p3-t2"));

        // add a ShutdownHook for the thread to invoke wakeup() method and wait until the main thread is terminated.
        addShutdownHookToCallWakeup(kafkaConsumer);

        consume(kafkaConsumer);
    }

    private static Properties buildProperties() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "34.64.226.236:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "multiple-topics-group");
        // props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private static KafkaConsumer<String, String> buildConsumer(Properties props, List<String> topicNames) {
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(topicNames);
        return kafkaConsumer;
    }

    private static void addShutdownHookToCallWakeup(KafkaConsumer<String, String> kafkaConsumer) {
        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("The wakeup thread starts to terminate consumer by calling wakeup() method.");
            kafkaConsumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }));
    }

    private static void consume(KafkaConsumer<String, String> kafkaConsumer) {
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000L));

                for (ConsumerRecord record : records) {
                    log.info("topic: {}, record key: {}, record value: {}, partition: {}, record offset: {}",
                            record.topic(),
                            record.key(),
                            record.value(),
                            record.partition(),
                            record.offset()
                    );
                }
            }
        } catch (WakeupException e) {
            log.warn("A WakeupException has been thrown.", e);
        } finally {
            log.info("The kafka consumer is closing.");
            kafkaConsumer.close();
        }
    }
}
