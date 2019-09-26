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
package org.apache.rocketmq.example.namespace;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/***
 * 注意 Producer：
 *    默认情况下不需要设置instanceName，rocketmq会使用ip@pid(pid代表jvm名字)作为唯一标示
 *
 *    如果同一个jvm中，不同的producer需要往不同的RocketMQ集群发送消息，需要设置不同的instanceName
 *    原因如下：如果不设置instanceName，那么会使用ip@pid作为producer唯一标识，那么会导致多个producer内部只有一个MQClientInstance(与mq交互)实例，从而导致只往一个集群发消息。
 */
public class ProducerWithNamespace {
    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("InstanceTest", "pidTest");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        for (int i = 0; i < 100; i++) {
            Message message = new Message("topicTest", "tagTest", "Hello world".getBytes());
            try {
                SendResult result = producer.send(message);
                System.out.printf("Topic:%s send success, misId is:%s%n", message.getTopic(), result.getMsgId());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}