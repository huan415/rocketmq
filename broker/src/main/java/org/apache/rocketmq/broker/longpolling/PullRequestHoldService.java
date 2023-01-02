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
package org.apache.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ConsumeQueueExt;

public class PullRequestHoldService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final String TOPIC_QUEUEID_SEPARATOR = "@";
    private final BrokerController brokerController;
    private final SystemClock systemClock = new SystemClock();
    //yangyc-main
    private ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable =
        new ConcurrentHashMap<String, ManyPullRequest>(1024);

    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    //yangyc-main 提交 pullRequest 请求
    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }
        //yangyc 将 pullRequest 加入到该 “主题@queueId”归属的 manyPullRequest 对象内部的 list 内
        mpr.addPullRequest(pullRequest);
    }

    private String buildKey(final String topic, final int queueId) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic);
        sb.append(TOPIC_QUEUEID_SEPARATOR);
        sb.append(queueId);
        return sb.toString();
    }

    //yangyc-main 内部程序执行方法
    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        //yangyc-main 死循环
        while (!this.isStopped()) {
            try {
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    //yangyc-main 服务器开启长轮询开关：每次循环休眠5s
                    this.waitForRunning(5 * 1000);
                } else {
                    //yangyc 服务器关闭长轮询开关：每次循环休眠1s
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }

                long beginLockTimestamp = this.systemClock.now();
                //yangyc-main 遍历处理 pullRequestTable
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;
                if (costTime > 5 * 1000) {
                    log.info("[NOTIFYME] check hold request cost {} ms.", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info("{} service end", this.getServiceName());
    }

    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }

    //yangyc-main 遍历处理 pullRequestTable
    private void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            //yangyc 循环体内为每个 topic@queueId k-v 的处理逻辑
            //yangyc-main key 按照@拆分，得到 topic 和 queueId
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0]; //yangyc 主题
                int queueId = Integer.parseInt(kArray[1]); //yangyc queueId
                //yangyc-main 根据主题和队列到存储模块查询该 ConsumeQueue 的最大 offset
                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                try {
                    //yangyc-main 通知消息到达的逻辑。参数1：主题，参数2：queueId，参数3：offset 当前 queue 最大 offset，
                    this.notifyMessageArriving(topic, queueId, offset);
                } catch (Throwable e) {
                    log.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
                }
            }
        }
    }
    //yangyc-main 通知消息到达的逻辑。参数1：主题，参数2：queueId，参数3：offset 当前 queue 最大 offset，
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        notifyMessageArriving(topic, queueId, maxOffset, null, 0, null, null);
    }

    //yangyc-main 通知消息到达
    //yangyc 该方法有两个调用点：
    // 1：pullRequestHoldService.run()
    // 2.ReputMessageService 异步构建 ConsumeQueue 和 index 的消息转发服务
    //yangyc 通知消息到达的逻辑。参数1：主题，参数2：queueId，参数3：offset 当前 queue 最大 offset，
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode,
        long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        //yangyc-main 构建 key, 规则：主题@queueId
        String key = this.buildKey(topic, queueId);
        //yangyc-main 获取 “主题#queueId” 的 manyPullRequest 对象
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {
            //yangyc-main 获取 ManyPullRequest 该 queue 下的 pullRequest list 数据, 并进行遍历
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                //yangyc 重放队列，当某个 pullRequest 即不超时，对应的queue的maxOffset <= pullRequest.offset 的话，就将该 pullRequest 再放入 replayList
                List<PullRequest> replayList = new ArrayList<PullRequest>();

                for (PullRequest request : requestList) {
                    long newestOffset = maxOffset;
                    //yangyc-main 检查 maxOffset>pullFromThisOffset 释放成立, 否则啥也不错
                    if (newestOffset <= request.getPullFromThisOffset()) {
                        //yangyc 保证 newestOffset 为 queue 的 maxOffset
                        newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                    }
                    //yangyc 条件成立：说明该 request 关注的 queue 内有本次 pull 查询的数据了，长轮询该结束了
                    if (newestOffset > request.getPullFromThisOffset()) {
                        boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                            new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                        // match by bit map, need eval again when properties is not null.
                        if (match && properties != null) {
                            match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                        }

                        if (match) {
                            try {
                                //yangyc-main 将满足条件的 pullRequest 再次封装出 RequestTask 提交到线程池内执行
                                // 会再次调用 PullMessageProcess.processRequest(...) 三个参数的方法，并且不允许再次长轮询。
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error("execute request when wakeup failed.", e);
                            }
                            continue;
                        }
                    }
                    //yangyc-main 判断该 pullRequest 是否长轮询超时，如果超时,也创建 RequestTask 提交到 BrokerController#pullMessageExecutor
                    // 超时时，会再次调用 PullMessageProcess.processRequest(...) 三个参数的方法，并且不允许再次长轮询。
                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error("execute request when wakeup failed.", e);
                        }
                        continue;
                    }
                    //yangyc 没有超时，也没有查询出结果
                    replayList.add(request);
                }

                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }
}
