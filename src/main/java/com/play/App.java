package com.play;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;

import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;

public class App 
{
    PulsarClient client;
    Consumer consumer;

    App(String topic, String sub) {
        try {
            this.client = PulsarClient.builder()
            .serviceUrl("pulsar://localhost:6650")
            .build();

            this.consumer = client.newConsumer()
            .topic(topic)
            .subscriptionName(sub)
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(60, TimeUnit.SECONDS)
            .negativeAckRedeliveryDelay(10L, TimeUnit.SECONDS)
            .deadLetterPolicy(DeadLetterPolicy.builder()
                .maxRedeliverCount(5)
                .deadLetterTopic("dlq-java")
                .build()
            )
            .subscribe();
        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }

    private static MessageId convertMessageIdForNack(MessageId messageId) {
        if (messageId instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) messageId;
            return new MessageIdImpl(batchMessageId.getLedgerId(), batchMessageId.getEntryId(), batchMessageId.getPartitionIndex());
        } else {
            return messageId;
        }
    }
    public static void main( String[] args )
    {
        App app = new App("my-topic", "test-sub");
        
        while (true) {
            try {
                Message msg = app.consumer.receive();
                System.out.println("message received: " + new String(msg.getData()));
                app.consumer.negativeAcknowledge(msg.getMessageId());
            } catch (Exception e) {

            }
        }
    }
}
