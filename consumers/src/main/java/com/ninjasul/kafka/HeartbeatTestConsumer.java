package com.ninjasul.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class HeartbeatTestConsumer {
    public static final Logger log = LoggerFactory.getLogger(HeartbeatTestConsumer.class);

    public static void main(String[] args) {
        Properties props = buildProperties();

        KafkaConsumer<String, String> kafkaConsumer = buildConsumer(props);

        consume(kafkaConsumer);
    }

    private static Properties buildProperties() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "34.64.226.236:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "heartbeat-group-01");
        props.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "5000");
        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "90000");
        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");
        return props;
    }

    private static KafkaConsumer<String, String> buildConsumer(Properties props) {
        String topicName = "heartbeat-topic";
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of(topicName));
        return kafkaConsumer;
    }

    private static void consume(KafkaConsumer<String, String> kafkaConsumer) {
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000L));

            for (ConsumerRecord record : records) {
                log.info("record key: {}, record value: {}, partition: {}", record.key(), record.value(), record.partition());
            }
        }
    }
}
