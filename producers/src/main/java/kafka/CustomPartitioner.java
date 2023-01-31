package kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CustomPartitioner implements Partitioner {
    public static final Logger log = LoggerFactory.getLogger(CustomPartitioner.class);

    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();

    private String specialKeyName;


    @Override
    public void configure(Map<String, ?> configs) {
        specialKeyName = configs.get(PizzaProducerWithCustomPartitioner.SPECIAL_KEY_NAME).toString();
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionsForTopic(topic).size();
        int numSpecialPartitions = numPartitions / 2;

        if (keyBytes == null) {
            //return stickyPartitionCache.partition(topic, cluster);
            throw new InvalidRecordException("key should not be null");
        }

        int partitionIndex = 0;
        if (key.equals(specialKeyName)) {
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % numSpecialPartitions;
        } else {
            partitionIndex = (Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - numSpecialPartitions)) + numSpecialPartitions;
        }

        log.info("message ({}:{}) is going to be sent to partition ({})", key, value.toString(), partitionIndex);
        return partitionIndex;
    }

    @Override
    public void close() {

    }
}
