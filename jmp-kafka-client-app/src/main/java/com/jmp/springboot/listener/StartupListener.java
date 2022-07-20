package com.jmp.springboot.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jmp.springboot.common.Constants;
import com.jmp.springboot.model.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class StartupListener implements ApplicationRunner {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String msg) {
        kafkaTemplate.send("test", msg);
    }

    @KafkaListener(topics = Constants.NOTIFICATION_TOPIC, groupId = Constants.CLIENT_GROUP)
    public void listen(String message) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Order order = objectMapper.readValue(message, Order.class);
        if (Constants.STATUS_DELIVERED.equals(order.getStatus())) {
            System.out.println("Received Message: " + message);
            System.out.println("Order is delivered !!!");
        }
    }
    @Override
    public void run(ApplicationArguments args) throws Exception {
        for (int i = 0; i < 1000; i++) {
//            sendMessage("Now: " + new Date());
//            Thread.sleep(2000);
        }
    }
}