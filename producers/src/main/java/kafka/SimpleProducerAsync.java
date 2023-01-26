package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducerAsync {
    public static final Logger log = LoggerFactory.getLogger(SimpleProducerAsync.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "34.64.226.236:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        String topicName = "simple-topic";
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "hello world5");

        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                log.info("\n ###### record metatdata received ###### \npartition: {}\noffset: {}\ntimestamp: {}",
                        metadata.partition(),
                        metadata.offset(),
                        metadata.timestamp()
                );
            } else {
                log.error("exception error from broker: {}", exception.getMessage());
            }
        });

        try {
            Thread.sleep(3000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }
}
