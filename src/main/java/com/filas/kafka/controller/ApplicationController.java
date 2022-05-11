package com.filas.kafka.controller;

import com.filas.kafka.model.Pessoa;
import com.filas.kafka.service.ApplicationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;

@Controller
public class ApplicationController {

    @Autowired
    ApplicationService applicationService;

    @GetMapping("/producer")
    public ResponseEntity<?> producerRun(@RequestBody Pessoa pessoa) {
        applicationService.sendMessage(pessoa);
        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }
}
