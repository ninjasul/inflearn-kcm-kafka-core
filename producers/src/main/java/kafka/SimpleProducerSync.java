package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimpleProducerSync {
    public static final Logger log = LoggerFactory.getLogger(SimpleProducerSync.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "34.64.226.236:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        String topicName = "simple-topic";
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "hello world3");

        Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
        try {
            RecordMetadata recordMetadata = future.get();

            log.info("\n ###### record metatdata received ###### \npartition: {}\noffset: {}\ntimestamp: {}",
                    recordMetadata.partition(),
                    recordMetadata.offset(),
                    recordMetadata.timestamp()
            );

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }
}
