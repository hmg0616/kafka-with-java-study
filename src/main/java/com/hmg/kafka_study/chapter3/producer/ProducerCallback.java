package com.hmg.kafka_study.chapter3.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerCallback implements Callback {
    private final static Logger logger = LoggerFactory.getLogger(ProducerCallback.class);

    // 레코드의 비동기 결과를 받기 위한 메서드
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null)
            logger.error(e.getMessage(), e); // 브로커 적재 실패 시 에러
        else
            logger.info(recordMetadata.toString());
    }
}