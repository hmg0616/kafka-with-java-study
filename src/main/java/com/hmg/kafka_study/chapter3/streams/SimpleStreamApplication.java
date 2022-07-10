package com.hmg.kafka_study.chapter3.streams;

import com.hmg.kafka_study.KafkaRunInterface;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class SimpleStreamApplication implements KafkaRunInterface {

    private static String APPLICATION_NAME = "streams-application"; // 애플리케이션 id 지정
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092"; // 카프카 클러스터 정보
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_COPY = "stream_log_copy";

    public void run() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());   // 키 직렬화, 역직렬화 방식 지정
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // 값 직렬화, 역직렬화 방식 지정

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(STREAM_LOG);  // 토픽으로부터 KStream 객체 만듦

        stream.to(STREAM_LOG_COPY); // KStream 데이터를 다른 토픽에 저장
        // stream.filter((key, value) -> value.length() > 5).to(STREAM_LOG_FILTER); // 필터링 처리 (문자열 길이 5보다 큰 경우)

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

    }
}
