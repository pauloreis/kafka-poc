package com.filas.kafka.service;

import com.filas.kafka.model.Pessoa;
import com.filas.kafka.producer.PessoaProducerImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class ApplicationService {
    @Autowired
    PessoaProducerImpl pessoaProducer;

    public void sendMessage(Pessoa pessoa){
        pessoaProducer.persist(UUID.randomUUID().toString(), pessoa);
    }
}
