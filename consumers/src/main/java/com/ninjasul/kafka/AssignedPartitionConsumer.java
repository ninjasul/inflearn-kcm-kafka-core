package com.ninjasul.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class AssignedPartitionConsumer {
    public static final Logger log = LoggerFactory.getLogger(AssignedPartitionConsumer.class);

    public static void main(String[] args) {
        Properties props = buildProperties();

        String topicName = "assigned-partition-topic";
        KafkaConsumer<String, String> kafkaConsumer = buildConsumer(props, topicName);

        // add a ShutdownHook for the thread to invoke wakeup() method and wait until the main thread is terminated.
        addShutdownHookToCallWakeup(kafkaConsumer);

        consume(kafkaConsumer);
    }

    private static Properties buildProperties() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "34.64.226.236:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "assigned-partition-group-01");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }

    private static KafkaConsumer<String, String> buildConsumer(Properties props, String topicName) {
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        kafkaConsumer.assign(Arrays.asList(topicPartition));
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
        polllWithCommitSync(kafkaConsumer);
    }

    private static void polllWithCommitSync(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCnt = 1;

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000L));
                log.info("####### loopCnt: {}, consumerRecords count: {} ######", loopCnt++, records.count());
                for (ConsumerRecord record : records) {
                    log.info("record key: {}, record value: {}, partition: {}, record offset: {}",
                            record.key(),
                            record.value(),
                            record.partition(),
                            record.offset()
                    );
                }

                try {
                    if (records.count() > 0) {
                        kafkaConsumer.commitSync();
                        log.info("commitSync() has been invoked.");
                    }
                } catch (CommitFailedException e) {
                    log.error("", e);
                }
            }
        } catch (WakeupException e) {
            log.warn("A WakeupException has been thrown.", e);
        } catch (Exception e) {
            log.error("", e);
        } finally {
            log.info("The kafka consumer is closing.");
            kafkaConsumer.close();
        }
    }
}
