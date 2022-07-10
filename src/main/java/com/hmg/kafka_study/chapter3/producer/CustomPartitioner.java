package com.hmg.kafka_study.chapter3.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    // 레코드를 기반으로 파티션을 정하는 로직. 리턴값은 주어진 레코드가 들어갈 파티션 번호.
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
                         Cluster cluster) {

        // 메시지 키를 지정하지 않은 경우 에러
        if (keyBytes == null) {
            throw new InvalidRecordException("Need message key");
        }
        // 메시지 키가 Pangyo일 경우 파티션 0번으로 지정되도록 0을 리턴
        if (((String)key).equals("Pangyo"))
            return 0;

        // 그외 메시지 키를 가진 레코드는 해시값을 지정하여 특정 파티션에 매칭되도록 설정
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }


    @Override
    public void configure(Map<String, ?> configs) {}

    @Override
    public void close() {}
}