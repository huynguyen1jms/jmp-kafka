package com.jmp.springboot.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.jmp.springboot.common.Constants;
import com.jmp.springboot.model.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class StartupListener implements ApplicationRunner {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String msg) {
        kafkaTemplate.send("test", msg);
    }

    @KafkaListener(topics = Constants.ORDER_TOPIC, groupId = Constants.CLIENT_GROUP)
    public void listen(String message) throws JsonProcessingException {
        System.out.println("Received Message: " + message);

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.WRAP_ROOT_VALUE, false);


        Order order = mapper.readValue(message, Order.class);
        order.setStatus(Constants.STATUS_READY);

        ObjectWriter ow = mapper.writer().withDefaultPrettyPrinter();
        String orderJson = ow.writeValueAsString(order);

        kafkaTemplate.send(Constants.NOTIFICATION_TOPIC, orderJson);
    }
    @Override
    public void run(ApplicationArguments args) throws Exception {
        for (int i = 0; i < 1000; i++) {
//            System.out.println("Send msg!!!");
//            sendMessage("Now: " + new Date());
//            Thread.sleep(2000);
        }
    }
}