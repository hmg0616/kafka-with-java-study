package com.hmg.kafka_study;

import com.hmg.kafka_study.chapter3.consumer.SimpleConsumer;
import com.hmg.kafka_study.chapter3.producer.SimpleProducer;
import com.hmg.kafka_study.chapter3.streams.KStreamJoinKTable;
import com.hmg.kafka_study.chapter3.streams.SimpleStreamApplication;

import java.util.function.Supplier;

public enum KafkaRunEnum {
    KafkaRunTest1(KafkaRunTest1::new),
    KafkaRunTest2(KafkaRunTest2::new),
    SimpleProducer(SimpleProducer::new),
    SimpleConsumer(SimpleConsumer::new),
    SimpleStreamApplication(SimpleStreamApplication::new),
    KStreamJoinKTable(KStreamJoinKTable::new);

    private Supplier<KafkaRunInterface> runClass;

    KafkaRunEnum(Supplier<KafkaRunInterface> runClass) {
        this.runClass = runClass;
    }

    public Supplier<KafkaRunInterface> getRunClass() {
        return runClass;
    }
}
