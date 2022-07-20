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

//    @KafkaListener(groupId = Constants.NOTIFICATION_GROUP,
//            topicPartitions = @TopicPartition(topic = Constants.NOTIFICATION_TOPIC,
//            partitions = "0"))
    @KafkaListener(topics = Constants.NOTIFICATION_TOPIC, groupId = Constants.NOTIFICATION_GROUP)
    public void listen(String message) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.WRAP_ROOT_VALUE, false);


        Order order = mapper.readValue(message, Order.class);

        if (Constants.STATUS_READY.equals(order.getStatus())) {
            System.out.println("Received Message: " + message);
            order.setStatus(Constants.STATUS_DELIVERED);

            ObjectWriter ow = mapper.writer().withDefaultPrettyPrinter();
            String orderJson = ow.writeValueAsString(order);
            kafkaTemplate.send(Constants.NOTIFICATION_TOPIC, orderJson);
        }

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