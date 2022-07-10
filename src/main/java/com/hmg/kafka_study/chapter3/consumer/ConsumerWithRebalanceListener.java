package com.hmg.kafka_study.chapter3.consumer;

import com.hmg.kafka_study.KafkaRunInterface;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class ConsumerWithRebalanceListener implements KafkaRunInterface { // 리밸런스 리스너를 가진 컨슈머

    private final static Logger logger = LoggerFactory.getLogger(ConsumerWithRebalanceListener.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";

    private static KafkaConsumer<String, String> consumer;

    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public void run() {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListener()); // 리밸런스 리스너 등록
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("{}", record);
                currentOffsets.put(
                        new TopicPartition(record.topic(), record.partition()), // 키 - 토픽, 파티션 정보
                        new OffsetAndMetadata(record.offset() + 1, null));     // 값 - 오프셋 정보
                consumer.commitSync(currentOffsets);
            }
        }
    }

    private static class RebalanceListener implements ConsumerRebalanceListener {
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) { // 리밸런스가 끝난 뒤 파티션 할당이 완료되면 호출되는 메서드
            logger.warn("Partitions are assigned : " + partitions.toString());
        }

        public void onPartitionsRevoked(Collection<TopicPartition> partitions) { // 리밸런스 시작되기 직전에 호출되는 메서드
            logger.warn("Partitions are revoked : " + partitions.toString());
            consumer.commitSync(currentOffsets); // 리밸런스 시작 직전 커밋 구현. 가장 마지막으로 처리 완료한 레코드 기준으로 커밋 실시.
        }
    }

}
