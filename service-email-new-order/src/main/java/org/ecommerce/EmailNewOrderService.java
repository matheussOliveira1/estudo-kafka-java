package org.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.ecommerce.consumer.ConsumerService;
import org.ecommerce.consumer.ServiceRunner;
import org.ecommerce.dispatcher.KafkaDispatcher;

import java.math.BigDecimal;
import java.util.concurrent.ExecutionException;

public class EmailNewOrderService implements ConsumerService<Order> {

    public static void main(String[] args) {
        new ServiceRunner<>(EmailNewOrderService::new).start(1);
    }

    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("-------------------------------------------");
        System.out.println("Processing new order, preparing email");

        var message = record.value();
        System.out.println(record.value());

        var emailCode = "Thank you for your order! We are processing your order!";
        var order = message.getPayload();
        var id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());
        emailDispatcher.send(
                "ECOMMERCE_SEND_EMAIL",
                order.getEmail(),
                id,
                emailCode
        );
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }

}
