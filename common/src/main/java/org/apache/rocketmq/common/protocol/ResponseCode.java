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

import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

public class ResponseCode extends RemotingSysResponseCode {

    //yangyc 刷新到磁盘超时
    public static final int FLUSH_DISK_TIMEOUT = 10;

    //yangyc 从节点不可达
    public static final int SLAVE_NOT_AVAILABLE = 11;

    //yangyc 从节点刷盘超时
    public static final int FLUSH_SLAVE_TIMEOUT = 12;

    //yangyc 非法的消息结构
    public static final int MESSAGE_ILLEGAL = 13;

    //yangyc 服务不可用
    public static final int SERVICE_NOT_AVAILABLE = 14;

    //yangyc 版本不支持
    public static final int VERSION_NOT_SUPPORTED = 15;

    //yangyc 未授权的
    public static final int NO_PERMISSION = 16;

    //yangyc Topic 不存在
    public static final int TOPIC_NOT_EXIST = 17;
    //yangyc Topic 已经存在
    public static final int TOPIC_EXIST_ALREADY = 18;
    //yangyc 要拉取的偏移量不存在
    public static final int PULL_NOT_FOUND = 19;

    //yangyc 立刻重新拉取
    public static final int PULL_RETRY_IMMEDIATELY = 20;

    //yangyc 重定向拉取的偏移量
    public static final int PULL_OFFSET_MOVED = 21;

    //yangyc 不存在的队列
    public static final int QUERY_NOT_FOUND = 22;

    //yangyc 订阅的 url 解析失败
    public static final int SUBSCRIPTION_PARSE_FAILED = 23;

    //yangyc 目标订阅不存在
    public static final int SUBSCRIPTION_NOT_EXIST = 24;

    //yangyc 订阅不是最新的
    public static final int SUBSCRIPTION_NOT_LATEST = 25;

    //yangyc 订阅组不存在
    public static final int SUBSCRIPTION_GROUP_NOT_EXIST = 26;

    //yangyc 订阅的数据不存在 (tag表达式异常)
    public static final int FILTER_DATA_NOT_EXIST = 27;

    //yangyc 该 Broker 上订阅的数据不是最新的
    public static final int FILTER_DATA_NOT_LATEST = 28;

    //yangyc 事务应该提交
    public static final int TRANSACTION_SHOULD_COMMIT = 200;

    //yangyc 事务应该回滚
    public static final int TRANSACTION_SHOULD_ROLLBACK = 201;

    //yangyc 事务状态位置
    public static final int TRANSACTION_STATE_UNKNOW = 202;

    //yangyc 事务状态Group错误
    public static final int TRANSACTION_STATE_GROUP_WRONG = 203;
    //yangyc 买家ID不存在
    public static final int NO_BUYER_ID = 204;

    public static final int NOT_IN_CURRENT_UNIT = 205;

    //yangyc 消费者不在线(rpc)
    public static final int CONSUMER_NOT_ONLINE = 206;

    //yangyc 消费超时
    public static final int CONSUME_MSG_TIMEOUT = 207;

    //yangyc 消息不存在
    public static final int NO_MESSAGE = 208;

    //yangyc 更新或创建 ACL 配置失败
    public static final int UPDATE_AND_CREATE_ACL_CONFIG_FAILED = 209;

    //yangyc 删除 ACL 配置失败
    public static final int DELETE_ACL_CONFIG_FAILED = 210;

    //yangyc 更新全局白名单地址失败
    public static final int UPDATE_GLOBAL_WHITE_ADDRS_CONFIG_FAILED = 211;

}
