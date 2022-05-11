package com.filas.kafka.consumer;

import com.filas.kafka.model.Pessoa;
import com.filas.kafka.model.PessoaAvro;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class PessoaConsumerImpl {

    // @KafkaListener(topics = "Pessoa", groupId = "pessoa-consumer")
    @KafkaListener(id = "pessoa-consumer", topicPartitions = {
            @TopicPartition(
                    topic = "Pessoa",
                    partitions = {"0"},
                    partitionOffsets = @PartitionOffset(partition = "*", initialOffset = "0")
            )
    })
    public void consume(@Payload PessoaAvro pessoaAvro){
        Pessoa pessoa = new Pessoa();
        pessoa.setNome(pessoaAvro.getNome().toString());
        pessoa.setSobreNome(pessoaAvro.getSobreNome().toString());
        System.out.println("Pessoa recebida: " + pessoa.getNome() + pessoa.getSobreNome());
    }
}
