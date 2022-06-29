package com.hmg.kafka_study;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaStudyApplication {

    public static void main(String[] args) {

        SpringApplication.run(KafkaStudyApplication.class, args);

        KafkaRunInterface kafkaRunInterface = KafkaRunEnum.valueOf(args[0]).getRunClass().get();
        kafkaRunInterface.run();
    }

}
