package com.hmg.kafka_study.chapter3.producer;

import com.hmg.kafka_study.KafkaRunInterface;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducer implements KafkaRunInterface {

    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    // 토픽 이름
    private final static String TOPIC_NAME = "test";
    // 카프카 클러스터 서버 host, IP 지정
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";

    @Override
    public void run() {

        // KafkaProducer 인스턴스를 생성하기 위한 프로듀서 옵션들 key,value 값으로 선언
        // 필수 옵션 반드시 선언, 선택 옵션은 선언하지 않으면 기본 옵션값으로 설정됨
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // 메시지 키 직렬화 클래스 선언
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // 메시지 값 직렬화 클래스 선언
//        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class); // 커스텀 파티셔너 지정

        // KafkaProducer 생성 시 파라미터로 Properties 전달.
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        String messageValue = "testMessage";
        // 카프카 브로커로 데이터 보내기 위해 ProducerRecord 생성.
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue); // 메시지만 전송
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageKey, messageValue); // 키, 메시지 전송
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, partitionNo, messageKey, messageValue); // 파티션 임의 지정
        
        // ProducerRecord 전송
        producer.send(record);
//        RecordMetadata metadata = producer.send(record).get(); // 프로듀서로 보낸 데이터 결과를 동기적으로 가져오기
//        producer.send(record, new ProducerCallback()); // 레코드 전송 후 비동기로 결과 받기

        logger.info("{}", record);
//        logger.info(metadata.toString());

        producer.flush(); // 내부 버퍼에 있는 레코드 배치를 브로커로 전송
        producer.close(); // 프로듀서 인스턴스 리소스 안전하게 종료
    }

}
