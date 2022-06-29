package com.hmg.kafka_study;

import java.util.function.Supplier;

public enum KafkaRunEnum {
    KafkaRunTest1(KafkaRunTest1::new),
    KafkaRunTest2(KafkaRunTest2::new);

    private Supplier<KafkaRunInterface> runClass;

    KafkaRunEnum(Supplier<KafkaRunInterface> runClass) {
        this.runClass = runClass;
    }

    public Supplier<KafkaRunInterface> getRunClass() {
        return runClass;
    }
}
