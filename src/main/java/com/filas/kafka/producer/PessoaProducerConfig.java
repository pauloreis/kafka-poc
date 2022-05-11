package com.filas.kafka.producer;

import com.filas.kafka.model.PessoaAvro;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class PessoaProducerConfig {
    @Bean
    public KafkaTemplate pessoaAvroTemplate(ProducerFactory<String, PessoaAvro> factory){
        return new KafkaTemplate(factory);
    }
}
