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
package org.apache.rocketmq.example.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionListenerImpl implements TransactionListener {

    private final AtomicInteger transactionIndex = new AtomicInteger(0);

    private final ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();

    /***
     * 执行本地事务方法
     * @param   msg Half(prepare) message
     * @param   arg Custom business parameter
     * @return  LocalTransactionState
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        // 提交本地事物 伪代码 提交事务成功成功之后 将状态保存起来 方便事务状态回查
        final int value = this.transactionIndex.getAndIncrement();
        final int status = value % 3;
        this.localTrans.put(msg.getTransactionId(), status);
        return LocalTransactionState.UNKNOW;
    }

    /***
     * 检查本地事务-回调再次处理
     * @param  msg Check message
     * @return LocalTransactionState
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        System.out.println("Msg:: " + new String(msg.getBody()) + " ID: " + msg.getMsgId());

        // 回调检查本地事物 伪代码
        final Integer status = this.localTrans.get(msg.getTransactionId());
        if (null != status) {
            switch (status) {
                case 0:
                    return LocalTransactionState.UNKNOW;
                case 1:
                    return LocalTransactionState.COMMIT_MESSAGE;
                case 2:
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                default:
                    return LocalTransactionState.COMMIT_MESSAGE;
            }
        }

        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
