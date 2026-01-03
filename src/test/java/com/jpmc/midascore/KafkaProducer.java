package com.jpmc.midascore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {

    private final String topic;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper mapper = new ObjectMapper();

    public KafkaProducer(@Value("${general.kafka-topic}") String topic,
                         KafkaTemplate<String, String> kafkaTemplate) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String transactionLine) {
        try {
            String[] d = transactionLine.split(", ");
            Transaction tx = new Transaction(
                    Long.parseLong(d[0]),
                    Long.parseLong(d[1]),
                    Float.parseFloat(d[2])
            );

            String json = mapper.writeValueAsString(tx);
            kafkaTemplate.send(topic, json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
