package com.filas.kafka.producer;

import com.filas.kafka.model.Pessoa;
import com.filas.kafka.model.PessoaAvro;
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
public class PessoaProducerImpl {
    @Autowired
    private KafkaTemplate<String, PessoaAvro> pessoaTemplate;
    private String topicName = "Pessoa";

    public void persist(String messageId, Pessoa pessoa){
        PessoaAvro pessoaAvro = createPessoaAvro(pessoa);
        sendPessoaMessage(messageId, pessoaAvro);
    }

    private void sendPessoaMessage(String messageId, PessoaAvro pessoaAvro) {
        var message =  createMessageWithHeaders(messageId, pessoaAvro, this.topicName);
        ListenableFuture<SendResult<String, PessoaAvro>> future = pessoaTemplate.send(message);



        future.addCallback(new ListenableFutureCallback<SendResult<String, PessoaAvro>>() {
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("FAIL: " + messageId);
            }

            @Override
            public void onSuccess(SendResult<String, PessoaAvro> result) {
                System.out.println("SUCCESS : " + messageId);
            }
        });
    }

    private Message<PessoaAvro> createMessageWithHeaders(String messageId, PessoaAvro pessoaAvro, String topic){
        return MessageBuilder.withPayload(pessoaAvro)
                .setHeader("hash", pessoaAvro.hashCode())
                .setHeader("version", "1.0.0")
                .setHeader("endOfLife", LocalDate.now().plusDays(1L))
                .setHeader("type", "fct")
                .setHeader("cid", messageId)
                .setHeader(KafkaHeaders.TOPIC, this.topicName)
                .setHeader(KafkaHeaders.MESSAGE_KEY, messageId)
                .build();
    }

    private PessoaAvro createPessoaAvro(Pessoa pessoa) {
        return PessoaAvro.newBuilder()
                .setNome(pessoa.getNome())
                .setSobreNome(pessoa.getSobreNome())
                .build();
    }
}
