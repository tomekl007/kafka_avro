package com.tomekl007.kafka.partitioning;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    private final String userWithDedicatedPartition;

    public CustomPartitioner(String userWithDedicatedPartition) {
        this.userWithDedicatedPartition = userWithDedicatedPartition;
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if ((keyBytes == null) || (!(key instanceof String)))
            throw new InvalidRecordException("We expect all messages to have String userId as a Key");
        if (key.equals(userWithDedicatedPartition))
            return numPartitions; //that specific user will always go to the last partition
        return (Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1));
    }
    //to enable it props.put("partitioner.class", "com.codenotfound.kafka.partitioning.CustomPartitioner");

    @Override
    public void close() {
    }
}

