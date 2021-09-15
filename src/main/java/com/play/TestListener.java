package com.play;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.*;

public class TestListener implements MessageListener<byte[]> {
    @Override
    public void received(Consumer<byte[]> consumer, Message<byte[]> message) {
        if (message == null) {
            return;
        }
        System.out.println("Message Received: " + new String(message.getData()));
        consumer.negativeAcknowledge(App.convertMessageIdForNack(message.getMessageId()));
    }
}
