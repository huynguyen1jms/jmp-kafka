package com.jmp.springboot.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.jmp.springboot.common.Constants;
import com.jmp.springboot.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.util.UUID;

@RequestMapping(value = "/order")
@Controller
public class ClientAppController {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

//    @Autowired
//    private EventService eventService;

    @Autowired
    private ObjectWriter objectWriter;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping(value = "/receive", method = RequestMethod.POST)
    public ResponseEntity<String> receiveOrder(@RequestBody String body) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.WRAP_ROOT_VALUE, false);


        Order order = mapper.readValue(body, Order.class);

        String id = UUID.randomUUID().toString().replace("-", "");
        order.setId(id);


        ObjectWriter ow = mapper.writer().withDefaultPrettyPrinter();
        String orderJson = ow.writeValueAsString(order);

        kafkaTemplate.send(Constants.ORDER_TOPIC, orderJson);

        return ResponseEntity.ok("sent order to order_topic");
    }

}
