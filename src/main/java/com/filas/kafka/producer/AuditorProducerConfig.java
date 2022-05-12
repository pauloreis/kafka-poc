package com.filas.kafka.producer;

import com.filas.kafka.model.AuditorAvro;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class AuditorProducerConfig {
    @Bean
    @Primary
    public KafkaTemplate auditorAvroTemplate(ProducerFactory<String, AuditorAvro> factory){
        return new KafkaTemplate(factory);
    }
}
