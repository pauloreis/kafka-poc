package com.filas.kafka.producer;

import com.filas.kafka.model.Auditor;
import com.filas.kafka.model.AuditorAvro;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.time.LocalDate;

@Component
public class AuditorProducerImpl {
    @Autowired
    private KafkaTemplate<String, AuditorAvro> auditorTemplate;
    private String topicName = "Auditor";

    public void persist(String messageId, Auditor auditor){
        AuditorAvro auditorAvro = createAuditorAvro(auditor);
        sendAuditorMessage(messageId, auditorAvro);
    }

    private void sendAuditorMessage(String messageId, AuditorAvro auditorAvro) {
        var message =  createMessageWithHeaders(messageId, auditorAvro, this.topicName);
        ListenableFuture<SendResult<String, AuditorAvro>> future = auditorTemplate.send(message);



        future.addCallback(new ListenableFutureCallback<SendResult<String, AuditorAvro>>() {
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("FAIL: " + messageId);
            }

            @Override
            public void onSuccess(SendResult<String, AuditorAvro> result) {
                System.out.println("SUCCESS : " + messageId);
            }
        });
    }

    private Message<AuditorAvro> createMessageWithHeaders(String messageId, AuditorAvro auditorAvro, String topic){
        return MessageBuilder.withPayload(auditorAvro)
                .setHeader("hash", auditorAvro.hashCode())
                .setHeader("version", "1.0.0")
                .setHeader("endOfLife", LocalDate.now().plusDays(1L))
                .setHeader("type", "fct")
                .setHeader("cid", messageId)
                .setHeader(KafkaHeaders.TOPIC, this.topicName)
                .setHeader(KafkaHeaders.MESSAGE_KEY, messageId)
                .build();
    }

    private AuditorAvro createAuditorAvro(Auditor auditor) {
        return AuditorAvro.newBuilder()
                .setId(auditor.getId())
                .setTransactionId(auditor.getTransactionId().toString())
                .setQtd(auditor.getQtd())
                .build();
    }
}
