package org.apache.rocketmq.example.transaction;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * ******************************
 * author：      柯贤铭
 * createTime:   2019/9/29 10:22
 * description:  TransactionMQConsumer
 * version:      V1.0
 * ******************************
 */
public class TransactionMQConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer mqPushConsumer = new DefaultMQPushConsumer("KerwinBoots");
        mqPushConsumer.setNamesrvAddr("127.0.0.1:9876");
        mqPushConsumer.subscribe("TransactionProducer", "*");
        mqPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        mqPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);

                int num = (int) (Math.random() * 10);
                if (num >= 5) {
                    System.out.println("Msg::Successful");
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }

                System.out.println("Msg::failure");
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });

        mqPushConsumer.start();
    }
}
