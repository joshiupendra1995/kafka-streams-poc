package com.kafka.poc.processor;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EventStreamProcessor {

    @Autowired
    private StreamsBuilder streamsBuilder;

    @PostConstruct
    public void streamTopology() {
        KStream<String, String> kStreamObj = streamsBuilder.stream("user", Consumed.with(Serdes.String(), Serdes.String()));
        kStreamObj.filter((k, v) -> v.startsWith("Message_"))
                .mapValues((k, v) -> v.toUpperCase())
                .peek((k, v) -> log.info("Key = {} and Value = {} ", k, v))
                .to("user-output", Produced.with(Serdes.String(), Serdes.String()));
    }
}
