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

package org.apache.rocketmq.common.protocol;

public class RequestCode {

    //yangyc 发送消息
    public static final int SEND_MESSAGE = 10;

    //yangyc 拉取消息
    public static final int PULL_MESSAGE = 11;

    //yangyc 查询消息(所在topic, 需要的 key, 最大数量, 开始偏移量, 结束偏移量)
    public static final int QUERY_MESSAGE = 12;
    //yangyc 查询 Broker 偏移量(未使用)
    public static final int QUERY_BROKER_OFFSET = 13;
    /*
     * 查询消费者偏移量
     *      消费者会将偏移量存储在内存中,当使用主从架构时,会默认由主 Broker 负责读于写
     *      为避免消息堆积,堆积消息超过指定的值时,会由从服务器来接管读,但会导致消费进度问题
     *      所以主从消费进度的一致性由 从服务器主动上报 和 消费者内存进度优先 来保证
     */
    //yangyc 查询指定队列的消费偏移量
    public static final int QUERY_CONSUMER_OFFSET = 14;
    //yangyc 提交自己的偏移量
    public static final int UPDATE_CONSUMER_OFFSET = 15;
    //yangyc 创建或更新Topic
    public static final int UPDATE_AND_CREATE_TOPIC = 17;
    //yangyc 获取所有的Topic信息
    public static final int GET_ALL_TOPIC_CONFIG = 21;
    public static final int GET_TOPIC_CONFIG_LIST = 22;

    public static final int GET_TOPIC_NAME_LIST = 23;

    //yangyc 更新 Broker 配置
    public static final int UPDATE_BROKER_CONFIG = 25;

    //yangyc 获取 Broker 配置
    public static final int GET_BROKER_CONFIG = 26;

    public static final int TRIGGER_DELETE_FILES = 27;

    //yangyc 获取 Broker 运行时信息
    public static final int GET_BROKER_RUNTIME_INFO = 28;
    //yangyc 通过时间戳查找偏移量
    public static final int SEARCH_OFFSET_BY_TIMESTAMP = 29;
    //yangyc 获取最大偏移量
    public static final int GET_MAX_OFFSET = 30;
    //yangyc 获取最小偏移量
    public static final int GET_MIN_OFFSET = 31;

    //yangyc 获取最早的存储消息时间
    public static final int GET_EARLIEST_MSG_STORETIME = 32;

    //yangyc-main       由 Broker 处理

    //yangyc 通过消息ID查询消息
    public static final int VIEW_MESSAGE_BY_ID = 33;

    //yangyc 心跳消息
    public static final int HEART_BEAT = 34;

    //yangyc 注销客户端
    public static final int UNREGISTER_CLIENT = 35;

    //yangyc 报告消费失败(一段时间后重试) (Deprecated)
    public static final int CONSUMER_SEND_MSG_BACK = 36;

    //yangyc 事务结果(可能是 commit 或 rollback)
    public static final int END_TRANSACTION = 37;
    //yangyc 通过消费者组获取消费者列表
    public static final int GET_CONSUMER_LIST_BY_GROUP = 38;

    //yangyc 检查事务状态; Broker对于事务的未知状态的回查操作
    public static final int CHECK_TRANSACTION_STATE = 39;

    //yangyc 通知消费者的ID已经被更改
    public static final int NOTIFY_CONSUMER_IDS_CHANGED = 40;

    //yangyc 批量锁定 Queue (rebalance使用)
    public static final int LOCK_BATCH_MQ = 41;

    //yangyc 批量解锁 Queue
    public static final int UNLOCK_BATCH_MQ = 42;
    //yangyc 获得该 Broker 上的所有的消费者偏移量
    public static final int GET_ALL_CONSUMER_OFFSET = 43;

    //yangyc 获得延迟 Topic 上的偏移量
    public static final int GET_ALL_DELAY_OFFSET = 45;

    //yangyc 检查客户端配置
    public static final int CHECK_CLIENT_CONFIG = 46;

    //yangyc 更新或创建 ACL
    public static final int UPDATE_AND_CREATE_ACL_CONFIG = 50;

    //yangyc 删除 ACL 配置
    public static final int DELETE_ACL_CONFIG = 51;

    //yangyc 获取 Broker 集群的 ACL 信息
    public static final int GET_BROKER_CLUSTER_ACL_INFO = 52;

    //yangyc 更新全局白名单
    public static final int UPDATE_GLOBAL_WHITE_ADDRS_CONFIG = 53;

    //yangyc 获取 Broker 集群的 ACL 配置
    public static final int GET_BROKER_CLUSTER_ACL_CONFIG = 54;

    //yangyc-main      NameServer 相关

    //yangyc 放入KV键值配置
    public static final int PUT_KV_CONFIG = 100;

    //yangyc 获取键值配置
    public static final int GET_KV_CONFIG = 101;

    //yangyc 删除KV键值配置
    public static final int DELETE_KV_CONFIG = 102;

    //yangyc 注册 Broker
    public static final int REGISTER_BROKER = 103;

    //yangyc 注销 Broker
    public static final int UNREGISTER_BROKER = 104;
    //yangyc 获取指定 Topic 的路由信息
    public static final int GET_ROUTEINFO_BY_TOPIC = 105;

    //yangyc 获取 Broker 的集群信息
    public static final int GET_BROKER_CLUSTER_INFO = 106;
    //yangyc 更新或创建订阅组
    public static final int UPDATE_AND_CREATE_SUBSCRIPTIONGROUP = 200;
    //yangyc 获取所有订阅组的配置
    public static final int GET_ALL_SUBSCRIPTIONGROUP_CONFIG = 201;
    //yangyc 获取 Topic 的度量指标
    public static final int GET_TOPIC_STATS_INFO = 202;
    //yangyc 获取消费者在线列表(rpc)
    public static final int GET_CONSUMER_CONNECTION_LIST = 203;
    //yangyc 获取生产者在线列表
    public static final int GET_PRODUCER_CONNECTION_LIST = 204;
    //yangyc 去除该broker上所有topic的写权限
    public static final int WIPE_WRITE_PERM_OF_BROKER = 205;

    //yangyc 从 NameSrv 获取所有 Topic
    public static final int GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 206;

    //yangyc 删除订阅组
    public static final int DELETE_SUBSCRIPTIONGROUP = 207;
    //yangyc 获取消费者的度量指标
    public static final int GET_CONSUME_STATS = 208;

    public static final int SUSPEND_CONSUMER = 209;

    public static final int RESUME_CONSUMER = 210;
    public static final int RESET_CONSUMER_OFFSET_IN_CONSUMER = 211;
    public static final int RESET_CONSUMER_OFFSET_IN_BROKER = 212;

    public static final int ADJUST_CONSUMER_THREAD_POOL = 213;

    public static final int WHO_CONSUME_THE_MESSAGE = 214;

    //yangyc 删除 Broker 中的 Topic
    public static final int DELETE_TOPIC_IN_BROKER = 215;

    //yangyc 删除 NameSrv 中的 Topic
    public static final int DELETE_TOPIC_IN_NAMESRV = 216;
    //yangyc 获取键值列表
    public static final int GET_KVLIST_BY_NAMESPACE = 219;

    //yangyc 重置消费者的消费进度
    public static final int RESET_CONSUMER_CLIENT_OFFSET = 220;

    //yangyc 从消费者中获取消费者的度量指标
    public static final int GET_CONSUMER_STATUS_FROM_CLIENT = 221;

    //yangyc 让 Broker 重置消费进度
    public static final int INVOKE_BROKER_TO_RESET_OFFSET = 222;

    //yangyc 让 Broker 更新消费者的度量信息
    public static final int INVOKE_BROKER_TO_GET_CONSUMER_STATUS = 223;

    //yangyc 查询消息被谁消费
    public static final int QUERY_TOPIC_CONSUME_BY_WHO = 300;

    //yangyc 从集群中获取 Topic
    public static final int GET_TOPICS_BY_CLUSTER = 224;

    //yangyc 注册过滤器服务器
    public static final int REGISTER_FILTER_SERVER = 301;
    //yangyc 注册消息过滤类
    public static final int REGISTER_MESSAGE_FILTER_CLASS = 302;

    //yangyc 查询消费时间
    public static final int QUERY_CONSUME_TIME_SPAN = 303;

    //yangyc 从 NameSrv 中获取系统Topic
    public static final int GET_SYSTEM_TOPIC_LIST_FROM_NS = 304;
    //yangyc 从 Broker 中获取系统Topic
    public static final int GET_SYSTEM_TOPIC_LIST_FROM_BROKER = 305;

    //yangyc 清理过期的消费队列
    public static final int CLEAN_EXPIRED_CONSUMEQUEUE = 306;

    //yangyc 获取 Consumer 的运行时信息
    public static final int GET_CONSUMER_RUNNING_INFO = 307;

    //yangyc 查询修正偏移量
    public static final int QUERY_CORRECTION_OFFSET = 308;
    //yangyc 直接消费消息
    public static final int CONSUME_MESSAGE_DIRECTLY = 309;

    //yangyc 发送消息(v2),优化网络数据包
    public static final int SEND_MESSAGE_V2 = 310;

    //yangyc 单元化相关 topic
    public static final int GET_UNIT_TOPIC_LIST = 311;

    //yangyc 获取含有单元化订阅组的 Topic 列表
    public static final int GET_HAS_UNIT_SUB_TOPIC_LIST = 312;

    //yangyc 获取含有单元化订阅组的非单元化 Topic 列表
    public static final int GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST = 313;

    //yangyc 克隆消费进度
    public static final int CLONE_GROUP_OFFSET = 314;

    //yangyc 查询 Broker 上的度量信息
    public static final int VIEW_BROKER_STATS_DATA = 315;

    //yangyc 清理未使用的 Topic
    public static final int CLEAN_UNUSED_TOPIC = 316;

    //yangyc 获取 broker 上的有关消费的度量信息
    public static final int GET_BROKER_CONSUME_STATS = 317;

    /**
     * update the config of name server
     */
    //yangyc 修改 NameServer 配置
    public static final int UPDATE_NAMESRV_CONFIG = 318;

    /**
     * get config from name server
     */
    //yangyc 获取 NameServer 配置
    public static final int GET_NAMESRV_CONFIG = 319;

    //yangyc 发送批量消息
    public static final int SEND_BATCH_MESSAGE = 320;

    //yangyc 查询消费的 Queue
    public static final int QUERY_CONSUME_QUEUE = 321;

    //yangyc 查询数据版本
    public static final int QUERY_DATA_VERSION = 322;

    /**
     * resume logic of checking half messages that have been put in TRANS_CHECK_MAXTIME_TOPIC before
     */
    //yangyc 检查半消息
    public static final int RESUME_CHECK_HALF_MESSAGE = 323;

    //yangyc 回送消息
    public static final int SEND_REPLY_MESSAGE = 324;

    //yangyc 回送消息
    public static final int SEND_REPLY_MESSAGE_V2 = 325;

    //yangyc push回送消息到客户端
    public static final int PUSH_REPLY_MESSAGE_TO_CLIENT = 326;
}
