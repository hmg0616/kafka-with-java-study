package com.hmg.kafka_study.chapter3.processorApi;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class FilterProcessor implements Processor<String, String> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) { // 스트림 프로세서 생성자. 프로세싱 처리에 필요한 리소스 선언.
        this.context = context;                  // ProcessorContext 초기화
    }

    @Override
    public void process(String key, String value) { // 실질적 프로세싱 로직
        if (value.length() > 5) {
            context.forward(key, value);
        }
        context.commit();
    }

    @Override
    public void close() { // FilterProcessor 종료 전 호출되는 메서드. 리소스 해제 구문 넣기.
    }

}
