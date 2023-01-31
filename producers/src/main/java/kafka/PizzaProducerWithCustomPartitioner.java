package kafka;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

public class PizzaProducerWithCustomPartitioner {
    public static final Logger log = LoggerFactory.getLogger(PizzaProducerWithCustomPartitioner.class);

    static final String SPECIAL_KEY_NAME = "custom.specialKey";
    static final String SPECIAL_KEY_VALUE = "P001";


    public static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer,
                                        String topicName, int iterCount,
                                        int interIntervalMillis, int intervalMillis,
                                        int intervalCount, boolean sync) {

        PizzaMessage pizzaMessage = new PizzaMessage();
        int iterSeq = 0;
        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        long startTime = System.currentTimeMillis();

        while( iterSeq++ != iterCount ) {
            HashMap<String, String> pMessage = pizzaMessage.produce(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,
                    pMessage.get("key"), pMessage.get("message"));
            sendMessage(kafkaProducer, producerRecord, pMessage, sync);

            if((intervalCount > 0) && (iterSeq % intervalCount == 0)) {
                try {
                    log.info("####### IntervalCount: {}, intervalMillis: {}  #########", intervalCount, intervalMillis);
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                }
            }

            if(interIntervalMillis > 0) {
                try {
                    log.info("interIntervalMillis:" + interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                }
            }

        }
        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;

        log.info("{} millisecond elapsed for {} iterations", timeElapsed, iterCount);
    }

    public static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                   ProducerRecord<String, String> producerRecord,
                                   HashMap<String, String> pMessage, boolean sync) {
        if(!sync) {
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    log.info("async message: {}, partition: {}, offset: {}", pMessage.get("key"), metadata.partition(), metadata.offset());
                } else {
                    log.error("exception error from broker {}", exception.getMessage());
                }
            });
        } else {
            try {
                RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
                log.info("sync message: {}, partition: {}, offset: {}", pMessage.get("key"), metadata.partition(), metadata.offset());
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        Properties props  = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "34.64.226.236:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, "kafka.CustomPartitioner");
        props.setProperty(SPECIAL_KEY_NAME, SPECIAL_KEY_VALUE);

        String topicName = "pizza-topic-partitioner";
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        sendPizzaMessage(kafkaProducer, topicName,
                -1, 100, 0, 0, true);

        kafkaProducer.close();
    }
}
