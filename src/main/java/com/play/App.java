package com.play;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;

import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;

public class App 
{
    // Consumer with Schema.
    Consumer<byte[]> consumer;
    // Consumer without Schema.
    Consumer consumerx;
    private String name;

    MessageListener listener = (consumer, msg) -> {
         try {
             System.out.println("Message received: " + new String(msg.getData()));
             consumer.negativeAcknowledge(msg.getMessageId());
         } catch (Exception e) {
             System.out.println("Encountered exception: " + e.toString());
         }
     };

    App(PulsarClient client, String name, String topic, String sub) {
        try {
            this.consumer = client.newConsumer(Schema.BYTES)
            .topic(topic)
            .subscriptionName(sub)
            .subscriptionType(SubscriptionType.Shared)
            .messageListener(new TestListener())
            .ackTimeout(60, TimeUnit.SECONDS)
            .negativeAckRedeliveryDelay(10L, TimeUnit.SECONDS)
            .deadLetterPolicy(DeadLetterPolicy.builder()
                .maxRedeliverCount(1)
                .deadLetterTopic("dlq-java")
                .build()
            )
            .subscribe();

            this.name = name;
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public App(PulsarClient client, String name, String topic, String sub, boolean withSchema) {
        try {
            this.consumerx = client.newConsumer(Schema.AUTO_CONSUME())
                    .topic(topic)
                    .subscriptionName(sub)
                    .subscriptionType(SubscriptionType.Shared)
//                    .messageListener(new TestListener())
                    .ackTimeout(300, TimeUnit.SECONDS)
                    .negativeAckRedeliveryDelay(10L, TimeUnit.SECONDS)
                    .deadLetterPolicy(DeadLetterPolicy.builder()
                            .maxRedeliverCount(1)
                            .deadLetterTopic("dlq-java")
                            .build()
                    )
                    .subscribe();

            this.name = name;
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public String getName() {
        return name;
    }

    public static MessageId convertMessageIdForNack(MessageId messageId) {
        if (messageId instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) messageId;
            return new MessageIdImpl(batchMessageId.getLedgerId(), batchMessageId.getEntryId(), batchMessageId.getPartitionIndex());
        } else {
            return messageId;
        }
    }

    public static void main( String[] args )
    {
        PulsarClient client = null;
        try {
            client = PulsarClient.builder()
                    .serviceUrl("pulsar://localhost:6650")
                    .build();
        } catch (Exception e) {
            System.err.println("error creating client: " + e.toString());
        }
        App app = new App(client, "dlq-app", "my-topic", "test-sub");
//        App app = new App(client, "dlq-app", "my-topic", "test-sub", true);
        System.out.println(app.getName() + " is running...");
        
//         while (true) {
//             try {
//                 Message msg = app.consumerx.receive();
//                 System.out.println("message received: " + new String(msg.getData()));
//                 app.consumerx.negativeAcknowledge(msg.getMessageId());
//             } catch (Exception e) {
//
//             }
//         }
    }
}
