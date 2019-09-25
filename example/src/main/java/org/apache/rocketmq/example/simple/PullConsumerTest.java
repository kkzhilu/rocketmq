/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.simple;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

/***
 * 直接通过设置偏移量进行消息获取
 */
public class PullConsumerTest {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("KerwinBoots");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.start();

        try {
            MessageQueue mq = new MessageQueue();
            mq.setQueueId(0);
            mq.setTopic("TopicTest");
            mq.setBrokerName("Kerwin");

            long offset = 1;
            long beginTime = System.currentTimeMillis();
            PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, offset, 32);
            System.out.printf("%s%n", System.currentTimeMillis() - beginTime);
            System.out.printf("%s%n", pullResult);
            if (pullResult.getMsgFoundList() != null) {
                for (MessageExt messageExt : pullResult.getMsgFoundList()) {
                    System.out.printf("%s%n", "MQ is: " + new String(messageExt.getBody()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        consumer.shutdown();
    }
}
