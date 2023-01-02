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
package org.apache.rocketmq.store.schedule;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

//yangyc-main 延迟消息
// ScheduleMessageService 会每隔 1 秒钟执行一个 executeOnTimeup 任务，将消息从延迟队列中写入正常 Topic 中。
public class ScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final long FIRST_DELAY_TIME = 1000L;
    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long DELAY_FOR_A_PERIOD = 10000L;
    //yangyc 存储延迟级别对应的 延迟时间长度（单位：毫秒）
    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
        new ConcurrentHashMap<Integer, Long>(32);
    //yangyc 存储延迟级别 queue 的消费进度 offset （该 table 每10s会持久化一次，持久化到本地磁盘）
    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
        new ConcurrentHashMap<Integer, Long>(32);
    //yangyc-main 存储主模块
    private final DefaultMessageStore defaultMessageStore;
    //yangyc-main 模块启动状态
    private final AtomicBoolean started = new AtomicBoolean(false);
    //yangyc-main 定时器，内部有线程池资源，可执行调度任务
    private Timer timer;
    private MessageStore writeMessageStore;
    //yangyc-main 最大延迟级别
    private int maxDelayLevel;

    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.writeMessageStore = defaultMessageStore;
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    /**
     * @param writeMessageStore
     *     the writeMessageStore to set
     */
    public void setWriteMessageStore(MessageStore writeMessageStore) {
        this.writeMessageStore = writeMessageStore;
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Map.Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    //yangyc-main 启动调度消息服务
    public void start() {
        //yangyc-main CAS 方式设置 started 状态为 true
        // CAS 操作保证了同一时间只会有一个 DeliverDelayedMessageTimerTask 执行。保证了消息安全的同时也限制了消息进行回传的效率.
        if (started.compareAndSet(false, true)) {
            //yangyc-main 创建定时器对象（内部有线程池资源）
            this.timer = new Timer("ScheduleMessageTimerThread", true);
            //yangyc-main 为每个延迟级别创建一个“延迟队列任务” 提交到 timer; 延迟 1s 后执行
            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
                Integer level = entry.getKey();
                Long timeDelay = entry.getValue();
                Long offset = this.offsetTable.get(level);
                if (null == offset) {
                    offset = 0L;
                }

                if (timeDelay != null) {
                    this.timer.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME);
                }
            }
            //yangyc-main 提交周期型任务，“持久化延迟队列消费进度任务”，延迟 10s 后执行，每10s执行一次
            this.timer.scheduleAtFixedRate(new TimerTask() {

                @Override
                public void run() {
                    try {
                        if (started.get()) ScheduleMessageService.this.persist();
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval());
        }
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            if (null != this.timer)
                this.timer.cancel();
        }

    }

    public boolean isStarted() {
        return started.get();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    public String encode() {
        return this.encode(false);
    }

    //yangyc-main 加载调度信息。两件事：1.初始化 delayLevelTable  2.初始化 offsetTable
    public boolean load() {
        boolean result = super.load();
        result = result && this.parseDelayLevel();
        return result;
    }

    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
            .getStorePathRootDir());
    }

    //yangyc-main 加载文件 ”..store/config/delayOffset.json“ 内容, 并初始化 offsetTable
    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                //yangyc-main 根据配置（MessageStoreConfig.messageDelayLevel） 初始化 delayLevelTable
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    public boolean parseDelayLevel() {
        //yangyc 该table存储 秒、分、小时、天 对应的毫秒值
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);
        //yangyc 默认支持的延迟级别
        // "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {
            //yangyc 按照空格拆分延迟级别成数组
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                //yangyc 获取出时间单位：“s”、“m”、“h”
                String ch = value.substring(value.length() - 1);
                //yangyc 获取时间单位对应的毫秒值
                Long tu = timeUnitTable.get(ch);
                //yangyc 延迟级别从 1 级 开始
                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    //yangyc 记录最大延迟级别
                    this.maxDelayLevel = level;
                }
                //yangyc 获取出数值
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                //yangyc 计算出延迟级别 延迟的总毫秒值
                long delayTimeMillis = tu * num;
                //yangyc 存储到 delayLevelTable; 方便后续使用
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    //yangyc-main ScheduleMessageService 会每隔 1 秒钟执行一个 executeOnTimeup 任务，将消息从延迟队列中写入正常 Topic 中。
    class DeliverDelayedMessageTimerTask extends TimerTask {
        private final int delayLevel; //yangyc-main 延迟队列任务 处理的 延迟级别
        private final long offset;    //yangyc-main 延迟队列任务 处理的 延迟队列的消费进度

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        //yangyc-main 任务执行入口
        @Override
        public void run() {
            try {
                if (isStarted()) {
                    this.executeOnTimeup();
                }
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                    this.delayLevel, this.offset), DELAY_FOR_A_PERIOD);
            }
        }

        /**
         * @return
         */
        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {

            long result = deliverTimestamp;
            //yangyc 计算出一个时间戳: now() + 延迟级别对应的延迟毫秒值
            long maxTimestamp = now + ScheduleMessageService.this.delayLevelTable.get(this.delayLevel);
            if (deliverTimestamp > maxTimestamp) { //yangyc 条件成立：说明 deliverTimestamp 是有问题的，这里调整为 now, 让外层立马将 msg 转发到 目标主题
                result = now;
            }
            //yangyc 一般情况 result == deliverTimestamp
            return result;
        }

        //yangyc-main ScheduleMessageService 会每隔 1 秒钟执行一个 executeOnTimeup 任务，将消息从延迟队列中写入正常 Topic 中。
        public void executeOnTimeup() {
            //yangyc-main 根据主题”SCHEDULE_TOPIC_XXXX“ 和 队列ID(delayLevel-1) 获取出该延迟队列任务 处理的 延迟队列 ConsumeQueue
            ConsumeQueue cq =
                ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                    delayLevel2QueueId(delayLevel));

            long failScheduleOffset = offset;

            if (cq != null) {
                //yangyc-main 根据消费进度 offset 查询出 SMBR 对象
                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
                if (bufferCQ != null) { //yangyc-main CASE1: SelectMappedBufferResult 内部有数据
                    try {
                        long nextOffset = offset;
                        int i = 0;
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        //yangyc-main 循环, 每次从 SMBR 中读取 20 个字节, 直到遍历完为止
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            //yangyc 读取20个字节，数据分别是：
                            long offsetPy = bufferCQ.getByteBuffer().getLong(); //yangyc 延迟消息的 物理偏移量
                            int sizePy = bufferCQ.getByteBuffer().getInt();     //yangyc 延迟消息的 消息大小
                            //yangyc 延迟消息的 交付时间（这个跟其他地方不一样，在ReputMessageService 转发时根据消息的 DELAY 属性是否>0， 会在tagsCode字段存储交付时间）
                            long tagsCode = bufferCQ.getByteBuffer().getLong(); //yangyc 延迟消息的 交付时间

                            if (cq.isExtAddr(tagsCode)) {
                                if (cq.getExt(tagsCode, cqExtUnit)) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    //can't find ext content.So re compute tags code.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}",
                                        tagsCode, offsetPy, sizePy);
                                    long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                                    tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                                }
                            }
                            //yangyc 获取系统当前时间
                            long now = System.currentTimeMillis();
                            //yangyc 获取延迟消息的交付时间
                            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);
                            //yangyc 下一条消息的 offset (CQData offset)
                            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                            //yangyc-main 差值
                            long countdown = deliverTimestamp - now;

                            if (countdown <= 0) { //yangyc-main 条件成立：说明 msg 已经到达交付时间了
                                //yangyc-main 根据 offsetPy 和 SizePy 读取出 延迟队列的这条消息，从 commitLog 文件
                                MessageExt msgExt =
                                    ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(
                                        offsetPy, sizePy);

                                if (msgExt != null) {
                                    try {
                                        //yangyc-main 根据延迟消息，重建一条新消息，字段大部分都是从上一步查询出的消息 cp 过来。。
                                        // 修改了一些字段：1.清理延迟级别属性（存在该属性且该属性>0,存储时会重定向到延迟级别，这里清理该属性避免该操作）
                                        // 2.从 properties 中读取属性”REAL_TOPIC“的值 3.从 properties 中读取属性”REAL_QID“的值
                                        MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);
                                        if (TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC.equals(msgInner.getTopic())) {
                                            log.error("[BUG] the real topic of schedule msg is {}, discard the msg. msg={}",
                                                    msgInner.getTopic(), msgInner);
                                            continue;
                                        }
                                        //yangyc-main 调用存储模块的 ”存储消息“ 方法 将新消息存储到 commitLog（最终ReputMessageService会向目标主题的ConsumeQueue中添加CQData）
                                        //yangyc 作为消费者 订阅的是 目标主题，所以会再次消费该消息
                                        PutMessageResult putMessageResult =
                                            ScheduleMessageService.this.writeMessageStore
                                                .putMessage(msgInner);

                                        if (putMessageResult != null
                                            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                                            continue;
                                        } else {
                                            // XXX: warn and notify me
                                            log.error(
                                                "ScheduleMessageService, a message time up, but reput it failed, topic: {} msgId {}",
                                                msgExt.getTopic(), msgExt.getMsgId());
                                            ScheduleMessageService.this.timer.schedule(
                                                new DeliverDelayedMessageTimerTask(this.delayLevel,
                                                    nextOffset), DELAY_FOR_A_PERIOD);
                                            ScheduleMessageService.this.updateOffset(this.delayLevel,
                                                nextOffset);
                                            return;
                                        }
                                    } catch (Exception e) {
                                        /*
                                         * XXX: warn and notify me



                                         */
                                        log.error(
                                            "ScheduleMessageService, messageTimeup execute error, drop it. msgExt="
                                                + msgExt + ", nextOffset=" + nextOffset + ",offsetPy="
                                                + offsetPy + ",sizePy=" + sizePy, e);
                                    }
                                }
                            } else {//yangyc-main 差值>0
                                //yangyc 执行到这里，说明 msg 还未到达交付时间
                                //yangyc-main 创建该"延迟消息任务”，offset 为最新值，延迟 countDown 毫秒之后在执行
                                // 提交 “延迟消息任务” 到timer, 延迟“差值”毫秒后执行。
                                ScheduleMessageService.this.timer.schedule(
                                    new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset),
                                    countdown);
                                //yangyc-main 更新延迟级别队列的消费进度
                                ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                return;
                            }
                        } // end of for

                        //yangyc-main 重建 “延迟消息任务”， offset 不变
                        // 提交 “延迟消息任务” 到 timer, 延迟100毫秒后执行。
                        nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                            this.delayLevel, nextOffset), DELAY_FOR_A_WHILE);
                        //yangyc-main 更新延迟级别队列的消费进度
                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                        return;
                    } finally {

                        bufferCQ.release();
                    }
                } // end of if (bufferCQ != null)
                else { //yangyc-main  CASE2: SelectMappedBufferResult == null

                    long cqMinOffset = cq.getMinOffsetInQueue();
                    if (offset < cqMinOffset) {
                        failScheduleOffset = cqMinOffset;
                        log.error("schedule CQ offset invalid. offset=" + offset + ", cqMinOffset="
                            + cqMinOffset + ", queueId=" + cq.getQueueId());
                    }
                }
            } // end of if (cq != null)
            //yangyc 重新提交 该延迟级别对应的 延迟队列任务，100ms之后再执行
            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel,
                failScheduleOffset), DELAY_FOR_A_WHILE);
        }

        private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
            //yangyc 新建一条空消息，新建消息大部分字段大部分字段都是从 “被延迟消息” copy 过来的
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());

            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            //yangyc 这里注意，tagsCodeValue 不再是交付时间
            long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            msgInner.setTagsCode(tagsCodeValue);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

            msgInner.setWaitStoreMsgOK(false);
            //yangyc 清理新消息的 DELAY 属性，为什么要清理呢？如果不清理，回头存储时，又会转发到调度主题了
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
            //yangyc 修改主题为 “%RETRY%GroupName”
            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));
            //yangyc 修改主题为 “0”
            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }
    }
}
