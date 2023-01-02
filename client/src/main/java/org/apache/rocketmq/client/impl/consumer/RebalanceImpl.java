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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

public abstract class RebalanceImpl {
    protected static final InternalLogger log = ClientLogger.getLog();
    //yangyc-main 分配到当前消费者的全部处理队列信息，key:messageQueue    value:processQueue(队列在消费者端的快照)
    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>(64);
    //yangyc-main 消费者订阅主题的队列信息
    protected final ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable =
        new ConcurrentHashMap<String, Set<MessageQueue>>();
    //yangyc-main 消费者订阅数据。key:主题   value:主题的过滤信息
    protected final ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner =
        new ConcurrentHashMap<String, SubscriptionData>();
    //yangyc-main 消费者组
    protected String consumerGroup;
    //yangyc-main 消费模式，默认：集群模式
    protected MessageModel messageModel;
    //yangyc-main 分配策略（1.平均分配策略、2.圆环平均分配策略、3.根据机房平均分配策略、4.根据机房平均分配策略加强版）
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    //yangyc-main 客户端实例
    protected MQClientInstance mQClientFactory;

    public RebalanceImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mQClientFactory = mQClientFactory;
    }

    public void unlock(final MessageQueue mq, final boolean oneway) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
                log.warn("unlock messageQueue. group:{}, clientId:{}, mq:{}",
                    this.consumerGroup,
                    this.mQClientFactory.getClientId(),
                    mq);
            } catch (Exception e) {
                log.error("unlockBatchMQ exception, " + mq, e);
            }
        }
    }

    public void unlockAll(final boolean oneway) {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        for (final Map.Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);

                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            processQueue.setLocked(false);
                            log.info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                        }
                    }
                } catch (Exception e) {
                    log.error("unlockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    private HashMap<String/* brokerName */, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
        HashMap<String, Set<MessageQueue>> result = new HashMap<String, Set<MessageQueue>>();
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            Set<MessageQueue> mqs = result.get(mq.getBrokerName());
            if (null == mqs) {
                mqs = new HashSet<MessageQueue>();
                result.put(mq.getBrokerName(), mqs);
            }

            mqs.add(mq);
        }

        return result;
    }

    public boolean lock(final MessageQueue mq) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                Set<MessageQueue> lockedMq =
                    this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
                for (MessageQueue mmqq : lockedMq) {
                    ProcessQueue processQueue = this.processQueueTable.get(mmqq);
                    if (processQueue != null) {
                        processQueue.setLocked(true);
                        processQueue.setLastLockTimestamp(System.currentTimeMillis());
                    }
                }

                boolean lockOK = lockedMq.contains(mq);
                log.info("the message queue lock {}, {} {}",
                    lockOK ? "OK" : "Failed",
                    this.consumerGroup,
                    mq);
                return lockOK;
            } catch (Exception e) {
                log.error("lockBatchMQ exception, " + mq, e);
            }
        }

        return false;
    }

    //yangyc-main 续约锁
    public void lockAll() {
        //yangyc-main 将分配给当前消费者的全部mq. 按照 brokerName 分组
        // key:brokerName    value:该 broker 上分配给当前消费者的 queue 集合
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        Iterator<Entry<String, Set<MessageQueue>>> it = brokerMqs.entrySet().iterator();
        //yangyc-main 遍历所有的分组
        while (it.hasNext()) {
            Entry<String, Set<MessageQueue>> entry = it.next();
            //yangyc brokerName
            final String brokerName = entry.getKey();
            //yangyc  该 broker 上分配给当前消费者的 queue 集合
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;
            //yangyc-main 查询 broker 主节点信息
            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                //yangyc-main 封装请求 body
                LockBatchRequestBody requestBody = new LockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    //yangyc-main 向 broker 发起 批量续约锁的请求。 返回值：续约锁成功的mq集合
                    Set<MessageQueue> lockOKMQSet =
                        this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
                    //yangyc-main CASE1: 遍历续约成功的 mq
                    for (MessageQueue mq : lockOKMQSet) {
                        //yangyc 获取 pq
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            if (!processQueue.isLocked()) {
                                log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                            }
                            //yangyc 占用分布式锁的 locked 设置成 true
                            processQueue.setLocked(true);
                            //yangyc 保存续约锁的时间为当前时间
                            processQueue.setLastLockTimestamp(System.currentTimeMillis());
                        }
                    }
                    for (MessageQueue mq : mqs) {
                        //yangyc-main CASE2: 条件成立：说明该 mq 续约锁失败
                        if (!lockOKMQSet.contains(mq)) {
                            ProcessQueue processQueue = this.processQueueTable.get(mq);
                            if (processQueue != null) {
                                //yangyc 占用分布式锁的 locked 设置成 false. 表示分布式锁尚未占用成功。。。这个时候消费任务不能消费
                                processQueue.setLocked(false);
                                log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("lockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }
    //yangyc-main 负载均衡方法 参数：是否是顺序消费
    public void doRebalance(final boolean isOrder) {
        //yangyc 获取消费者订阅数据
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            //yangyc-main 遍历消费者订阅的每一个主题
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                //yangyc 订阅主题
                final String topic = entry.getKey();
                try {
                    //yangyc-main 按照主题进行负载均衡。参数1：主题；参数2：是否有序
                    this.rebalanceByTopic(topic, isOrder);
                } catch (Throwable e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("rebalanceByTopic Exception", e);
                    }
                }
            }
        }

        this.truncateMessageQueueNotMyTopic();
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }

    //yangyc-main 按照主题进行负载均衡。参数1：主题；参数2：是否有序
    private void rebalanceByTopic(final String topic, final boolean isOrder) {
        switch (messageModel) {
            case BROADCASTING: {
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                if (mqSet != null) {
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                    if (changed) {
                        this.messageQueueChanged(topic, mqSet, mqSet);
                        log.info("messageQueueChanged {} {} {} {}",
                            consumerGroup,
                            topic,
                            mqSet,
                            mqSet);
                    }
                } else {
                    log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                }
                break;
            }
            case CLUSTERING: { //yangyc 一般都是集群模式
                //yangyc-main 获取当前处理 ”主题“ 全部 MessageQueue, 从 topicSubscribeInfoTable 获得（该table启动阶段就初始化数据了, 并且客户端也会定时更新该table数据）
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                //yangyc-main 获取当前处理 "消费者组" 全部消费者id集合（从服务器获取）
                List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
                if (null == mqSet) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                }

                if (null == cidAll) {
                    log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                }

                if (mqSet != null && cidAll != null) {
                    List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
                    mqAll.addAll(mqSet);

                    //yangyc-main 主题mq集合 和 消费者ID集合 都进行排序操作。 目的：每个消费者视图一致性
                    Collections.sort(mqAll); //yangyc 队列排序
                    Collections.sort(cidAll); //yangyc 客户端id排序
                    //yangyc 队列分配策略
                    AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;
                    //yangyc 分配策略 分配结果
                    List<MessageQueue> allocateResult = null;
                    try {
                        //yangyc-main 调用队列分配策略，给当前消费者进行分配mq， 返回值：分配给当前消费者的队列集合
                        // 参数1:消费者组，参数2：当前消费者id，参数3：主题全部队列集合（包括所有broker上该主题的mq）,参数4：全部消费者id集合。
                        allocateResult = strategy.allocate(
                            this.consumerGroup,
                            this.mQClientFactory.getClientId(),
                            mqAll,
                            cidAll);
                    } catch (Throwable e) {
                        log.error("AllocateMessageQueueStrategy.allocate Exception. allocateMessageQueueStrategyName={}", strategy.getName(),
                            e);
                        return;
                    }

                    Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
                    if (allocateResult != null) {
                        allocateResultSet.addAll(allocateResult);
                    }
                    //yangyc-main 根据分配给当前消费者的”队列集合“做更新操作
                    // 返回值：true表示分配给当前消费者的队列发生变化，false表示无变化 参数1:主题，参数2：分配给当前消费者的结果，参数3：是否有序
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
                    if (changed) {
                        log.info(
                            "rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                            strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(), mqSet.size(), cidAll.size(),
                            allocateResultSet.size(), allocateResultSet);
                        this.messageQueueChanged(topic, mqSet, allocateResultSet);
                    }
                }
                break;
            }
            default:
                break;
        }
    }

    private void truncateMessageQueueNotMyTopic() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();

        for (MessageQueue mq : this.processQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }
    }
    //yangyc-main 根据分配给当前消费者的”队列集合“做更新操作
    // 返回值：true表示分配给当前消费者的队列发生变化，false表示无变化 参数1:主题，参数2：分配给当前消费者的结果，参数3：是否有序
    private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet,
        final boolean isOrder) {
        boolean changed = false; //yangyc 当前消费者的队列是否有变化

        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        //yangyc-main 计算出本次负载均衡后, 当前消费者 当前主题 被转移走的队列
        // 对于这些被转移到其他消费者的队列，当前消费者需要：
        // 1.将 mq -> pq 状态设置为 删除 状态
        // 2.持久化消费进度 + 删除本地该 mq 的消费进度
        // 3.ProcessQueueTable 中删除该条 k-v。
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey(); //yangyc 队列元信息
            ProcessQueue pq = next.getValue(); //yangyc 队列在消费者端的快照

            if (mq.getTopic().equals(topic)) { //yangyc 条件成立：说明该mq是本地 rebalanceImpl 分配算法计算的主题
                if (!mqSet.contains(mq)) { //yangyc 条件成立：msqSet是最新分配给当前消费者的结果（指定主题）。说明该 mq 分配给了其他 consumer 节点
                    pq.setDropped(true); //yangyc 将pq删除状态设置成 true，消费任务会一种检查该状态，如果该状态变为删除状态，消费任务会立马退出
                    if (this.removeUnnecessaryMessageQueue(mq, pq)) { //yangyc 将当前队列相关信息移除走：1.消费进度持久化；2.offsetStore 内当前msq的进度删除，并返回true
                        it.remove(); //yangyc 从 processQueueTable 移除kv
                        changed = true; //yangyc 标记当前消费者 消费的队列发生了变化
                        log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                    }
                } else if (pq.isPullExpired()) { //yangyc 条件成立：1.当前mq还归属当前消费者，2.说明pq已经2min未到服务器拉消息了，可能出现了bug
                    switch (this.consumeType()) {
                        case CONSUME_ACTIVELY:
                            break;
                        case CONSUME_PASSIVELY:
                            pq.setDropped(true); //yangyc 设置pq删除状态为 true, 消费任务检查后会退出
                            //yangyc 将当前队列相关信息移除走：1.消费进度持久化；2.offsetStore 内当前msq的进度删除
                            if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                                it.remove(); //yangyc 移除当前kv
                                changed = true;
                                //yangyc 出了问题怎么办？相当重启。。。于让下一次rebalance分配到其他节点
                                log.error("[BUG]doRebalance, {}, remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                                    consumerGroup, mq);
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        //yangyc-main 计算出本次负载均衡后, 新分配到当前消费给、该主题的队列
        // 对于这些新分配给当前消费者该主题的队列，当前消费者需要：
        // 1.创建 ProcessQueue 为每个新分配的队列
        // 2.获取新分配的队列 的消费进度（offset）, 获取方式：到队列归属的 broker 上拉取
        // 3.ProcessQueueTable 添加 k-v, key: messageQueue  value:processQueue
        // 4.为新分配队列，创建 PullRequest 对象（封装：消费者组、mq、pq、消费进度）
        // 5.上一步创建的 PullRequest 对象转交给 PullMessageService (拉取消息服务)。
        //yangyc 拉消息请求列表（最终将它交给 PullMessageService 的队列内）
        List<PullRequest> pullRequestList = new ArrayList<PullRequest>();
        for (MessageQueue mq : mqSet) {
            if (!this.processQueueTable.containsKey(mq)) { //yangyc 条件成立：说明当前mq是RebalanceImpl之后，新分配给当前 consumer 的队列
                //yangyc 顺序消费首先需要获取新分配队列的分布式锁
                if (isOrder && !this.lock(mq)) {
                    log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                    //yangyc 获取锁失败、直接 continue
                    continue;
                }
                //yangyc 删除冗余数据（脏数据） offset
                this.removeDirtyOffset(mq);
                //yangyc 为新分配到当前消费者的mq 创建pq(快照队列)
                ProcessQueue pq = new ProcessQueue();
                //yangyc 从服务器拉取mq最新的消费进度
                long nextOffset = this.computePullFromWhere(mq);
                if (nextOffset >= 0) {
                    //yangyc 保存kv. key:messageQueue   value：上一步创建的mq对应的pq
                    ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                    } else { //yangyc 因为新创建的，所以pre是null
                        log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                        //yangyc 拉消息服务依赖于 pullRequest 对象进行拉消息工作，新分配的队列要创建新的pullRequest对象，最终放入拉消息服务的本地阻塞队列内。
                        PullRequest pullRequest = new PullRequest();
                        pullRequest.setConsumerGroup(consumerGroup);
                        //yangyc 从服务器拉取mq的消费进度，赋值给pullRequest
                        pullRequest.setNextOffset(nextOffset);
                        pullRequest.setMessageQueue(mq);
                        pullRequest.setProcessQueue(pq);
                        pullRequestList.add(pullRequest);
                        changed = true; //yangyc 标记当前消费者 消费的队列发生了变化
                    }
                } else {
                    log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                }
            }
        }
        //yangyc pullRequestList加入到queue
        this.dispatchPullRequest(pullRequestList);

        return changed;
    }

    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
        final Set<MessageQueue> mqDivided);

    public abstract boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

    public abstract ConsumeType consumeType();

    public abstract void removeDirtyOffset(final MessageQueue mq);

    public abstract long computePullFromWhere(final MessageQueue mq);

    public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList);

    public void removeProcessQueue(final MessageQueue mq) {
        ProcessQueue prev = this.processQueueTable.remove(mq);
        if (prev != null) {
            boolean droped = prev.isDropped();
            prev.setDropped(true);
            this.removeUnnecessaryMessageQueue(mq, prev);
            log.info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, droped);
        }
    }

    public ConcurrentMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    public ConcurrentMap<String, Set<MessageQueue>> getTopicSubscribeInfoTable() {
        return topicSubscribeInfoTable;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void destroy() {
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            next.getValue().setDropped(true);
        }

        this.processQueueTable.clear();
    }
}
