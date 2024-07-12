package com.bdreader.bdreader.rabbitmq;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;


@Service
public class RabbitMQSender {
    private final RabbitTemplate rabbitTemplate;

    @Value("${spring.rabbitmq.exchange}")
    private String exchange;

    @Value("${spring.rabbitmq.routingkey}")
    private String routingKey;

    public RabbitMQSender(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public Mono<Void> send(String message, LocalDateTime notificationTime) {

        long delay = notificationTime.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli() - System.currentTimeMillis();

        Map<String, Object> headers = new HashMap<>();
        headers.put("x-delay", delay);

        return Mono.fromRunnable(() -> {
            rabbitTemplate.convertAndSend(exchange, routingKey, message, m -> {
                m.getMessageProperties().getHeaders().putAll(headers);
                return m;
            });
        });
    }
}
