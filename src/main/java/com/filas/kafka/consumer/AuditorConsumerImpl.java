package com.filas.kafka.consumer;

import com.filas.kafka.model.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
public class AuditorConsumerImpl {

    @KafkaListener(topics = "Auditor", groupId = "auditor-consumer")
    public void consume(@Payload AuditorAvro auditorAvro){
        List<Coupon> coupons = new ArrayList<>();
        Auditor auditor = new Auditor();
        auditor.setId(auditorAvro.getId());
        auditor.setTransactionId(UUID.fromString(auditorAvro.getTransactionId().toString()));
        Integer qtd = auditorAvro.getQtd();
        System.out.println("Consumindo Auditor: ");
        for(int i=0; i<qtd; i++){
            Coupon coupon = new Coupon();
            coupon.setId(auditor.getId());
            coupon.setTransactionId(UUID.fromString(auditor.getTransactionId().toString()));
            coupon.setNumero(new Random().nextLong());
            coupons.add(coupon);
        }

        coupons.stream().forEach(p -> System.out.println(p.getId()+", "+p.getNumero()+", "+p.getTransactionId()));
    }
}
