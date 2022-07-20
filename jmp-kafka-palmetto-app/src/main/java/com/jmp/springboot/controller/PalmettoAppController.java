package com.jmp.springboot.controller;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.jmp.springboot.common.Constants;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@RequestMapping(value = "/order")
@Controller
public class PalmettoAppController {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private ObjectWriter objectWriter;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping(value = "/receive", method = RequestMethod.POST)
    public ResponseEntity<String> receiveOrder(@RequestBody String order) {
        ProducerRecord<String, String> record = new ProducerRecord<>(Constants.NOTIFICATION_TOPIC, 0, "", "test value");

        kafkaTemplate.send(record);


        return ResponseEntity.ok("sent order to order_topic");
    }
}
