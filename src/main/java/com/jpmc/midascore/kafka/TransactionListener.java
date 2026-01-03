package com.jpmc.midascore.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    private final ObjectMapper mapper = new ObjectMapper();

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-core-consumer"
    )
    public void receive(String message) throws Exception {
        Transaction tx = mapper.readValue(message, Transaction.class);
        System.out.println("RECEIVED TX = " + tx.getAmount());
    }
}
