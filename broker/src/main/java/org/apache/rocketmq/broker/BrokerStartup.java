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
package org.apache.rocketmq.broker;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_ENABLE;

public class BrokerStartup {
    public static Properties properties = null;
    public static CommandLine commandLine = null;
    public static String configFile = null;
    public static InternalLogger log;

    //yangyc-main
    // 1. 创建 BrokerController
    //     1. 组装几个核心的配置对象。 1.BrokerConfig、2.NettyServerConfig、3.NettyClientConfig、4.MessageStoreConfig
    //     2. 创建 Controller
    //     3. 初始化 Controller（controller.initialize()）.
    //         1. 初始化网络通信组件（remotingServer---向nettyServer注册处理器、fastRemotingServer、各种线程池）
    //         2. 初始化业务功能组件
    //             1. messageStore 创建并加载分配策略、加载存储文件
    //             2. brokerOuterAPI 网络请求组件
    // 2. 启动 BrokerController
    //     1. 启动之前初始化的一大堆组件
    //         1. messageStore 启动核心的消息存储组件
    //            1.启动一系列服务
    //                1. reputMessageService 启动分发服务
    //                2. haService 启动 haService
    //                3. flushConsumeQueueService 启动消费队列刷盘服务
    //                4. commitLog 启动 commitLog 模块（主要是启动 commitLog 内容的刷盘服务 flushCommitLogService）
    //                5. storeStatsService 启动 storeStatsService。
    //            2. commitLog#topicQueueTable 再次构建队列偏移量字典表
    //            3. 创建 abort 文件, 正常关机时, JVM HOOK 会删除该文件, 宕机时该文件不会被删除, 恢复阶段根据该文件是否存在, 执行不同的恢复策略
    //            4. 添加定时任务
    //                1.  ”清理过期文件“ 定时任务（commitLog、consumeQueue） DefaultMessageStore.this.cleanFilesPeriodically();
    //                2. ”磁盘预警“ 定时任务 DefaultMessageStore.this.cleanCommitLogService.isSpaceFull();.
    //                2. DefaultMessageStore.this.checkSelf();
    //                3. DefaultMessageStore.this.commitLog.getBeginTimeInLock()。
    //         2. remotingServer 启动 nettyServer
    //            1.server端和client  有一个定时器 , 每 1s 执行一次，扫描 scanResponseTable 表，将过期的 responseFuture 移除。
    //         3. fastRemotingServer 启动  快速通道nettyServer
    //         4. fileWatchService 启动 ssl文件观察服务.
    //         5. brokerOuterAPI 启动客户端, 往外发送请求
    //            1. NettyRemotingClient 启动.
    //         6. pullRequestHoldService 启动 长轮询实现 服务
    //            1. run() 死循环.
    //         7. clientHousekeepingService 启动 心跳服务 服务
    //            1. run() 死循环   ClientHousekeepingService.this.scanExceptionChannel();
    //         8. filterServerManager 启动 过滤服务器管理器 服务
    //            1. run() 死循环   FilterServerManager.this.createFilterServer();
    //         9. brokerFastFailure 启动 快速故障 服务，处理broker发送后长时间未处理的请求.
    //     2.定时注册心跳 (启动时 + 心跳)。
    public static void main(String[] args) {
        start(createBrokerController(args));
    }

    //yangyc-main. 启动 BrokerController
    //     1. 启动之前初始化的一大堆组件
    //         1. messageStore 启动核心的消息存储组件
    //            1.启动一系列服务
    //                1. reputMessageService 启动分发服务
    //                2. haService 启动 haService
    //                3. flushConsumeQueueService 启动消费队列刷盘服务
    //                4. commitLog 启动 commitLog 模块（主要是启动 commitLog 内容的刷盘服务 flushCommitLogService）
    //                5. storeStatsService 启动 storeStatsService。
    //            2. commitLog#topicQueueTable 再次构建队列偏移量字典表
    //            3. 创建 abort 文件, 正常关机时, JVM HOOK 会删除该文件, 宕机时该文件不会被删除, 恢复阶段根据该文件是否存在, 执行不同的恢复策略
    //            4. 添加定时任务
    //                1.  ”清理过期文件“ 定时任务（commitLog、consumeQueue） DefaultMessageStore.this.cleanFilesPeriodically();
    //                2. ”磁盘预警“ 定时任务 DefaultMessageStore.this.cleanCommitLogService.isSpaceFull();.
    //                2. DefaultMessageStore.this.checkSelf();
    //                3. DefaultMessageStore.this.commitLog.getBeginTimeInLock()。
    //         2. remotingServer 启动 nettyServer
    //            1.server端和client  有一个定时器 , 每 1s 执行一次，扫描 scanResponseTable 表，将过期的 responseFuture 移除。
    //         3. fastRemotingServer 启动  快速通道nettyServer
    //         4. fileWatchService 启动 ssl文件观察服务.
    //         5. brokerOuterAPI 启动客户端, 往外发送请求
    //            1. NettyRemotingClient 启动.
    //         6. pullRequestHoldService 启动 长轮询实现 服务
    //            1. run() 死循环.
    //         7. clientHousekeepingService 启动 心跳服务 服务
    //            1. run() 死循环   ClientHousekeepingService.this.scanExceptionChannel();
    //         8. filterServerManager 启动 过滤服务器管理器 服务
    //            1. run() 死循环   FilterServerManager.this.createFilterServer();
    //         9. brokerFastFailure 启动 快速故障 服务，处理broker发送后长时间未处理的请求.
    //     2.定时注册心跳 (启动时 + 心跳)。
    public static BrokerController start(BrokerController controller) {
        try {

            //yangyc-main
            controller.start();

            String tip = "The broker[" + controller.getBrokerConfig().getBrokerName() + ", "
                + controller.getBrokerAddr() + "] boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();

            if (null != controller.getBrokerConfig().getNamesrvAddr()) {
                tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
            }

            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static void shutdown(final BrokerController controller) {
        if (null != controller) {
            controller.shutdown();
        }
    }

    //yangyc-main
    // 1. 创建 BrokerController
    //     1. 组装几个核心的配置对象。 1.BrokerConfig、2.NettyServerConfig、3.NettyClientConfig、4.MessageStoreConfig
    //     2. 创建 Controller
    //     3. 初始化 Controller（controller.initialize()）.
    //         1. 初始化网络通信组件（remotingServer---向nettyServer注册处理器、fastRemotingServer、各种线程池）
    //         2. 初始化业务功能组件
    //             1. messageStore 创建并加载分配策略、加载存储文件
    //             2. brokerOuterAPI 网络请求组件
    public static BrokerController createBrokerController(String[] args) {
        //yangyc-main 组装几个核心的配置对象。 1.BrokerConfig、2.NettyServerConfig、3.NettyClientConfig、4.MessageStoreConfig
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE)) {
            NettySystemConfig.socketSndbufSize = 131072;
        }

        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE)) {
            NettySystemConfig.socketRcvbufSize = 131072;
        }

        try {
            //PackageConflictDetect.detectFastjson();
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            commandLine = ServerUtil.parseCmdLine("mqbroker", args, buildCommandlineOptions(options),
                new PosixParser());
            if (null == commandLine) {
                System.exit(-1);
            }

            final BrokerConfig brokerConfig = new BrokerConfig();
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            final NettyClientConfig nettyClientConfig = new NettyClientConfig();

            nettyClientConfig.setUseTLS(Boolean.parseBoolean(System.getProperty(TLS_ENABLE,
                String.valueOf(TlsSystemConfig.tlsMode == TlsMode.ENFORCING))));
            nettyServerConfig.setListenPort(10911);
            final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

            if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
                int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
                messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
            }

            if (commandLine.hasOption('c')) { //yangyc -c 指定配置文件
                String file = commandLine.getOptionValue('c');
                if (file != null) {
                    configFile = file;
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);

                    properties2SystemEnv(properties);
                    MixAll.properties2Object(properties, brokerConfig);
                    MixAll.properties2Object(properties, nettyServerConfig);
                    MixAll.properties2Object(properties, nettyClientConfig);
                    MixAll.properties2Object(properties, messageStoreConfig);

                    BrokerPathConfigHelper.setBrokerConfigPath(file);
                    in.close();
                }
            }

            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);

            if (null == brokerConfig.getRocketmqHome()) {
                System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation", MixAll.ROCKETMQ_HOME_ENV);
                System.exit(-2);
            }

            String namesrvAddr = brokerConfig.getNamesrvAddr();
            if (null != namesrvAddr) {
                try {
                    String[] addrArray = namesrvAddr.split(";");
                    for (String addr : addrArray) {
                        RemotingUtil.string2SocketAddress(addr);
                    }
                } catch (Exception e) {
                    System.out.printf(
                        "The Name Server Address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168.0.1:9876\"%n",
                        namesrvAddr);
                    System.exit(-3);
                }
            }

            switch (messageStoreConfig.getBrokerRole()) {
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    brokerConfig.setBrokerId(MixAll.MASTER_ID);
                    break;
                case SLAVE:
                    if (brokerConfig.getBrokerId() <= 0) {
                        System.out.printf("Slave's brokerId must be > 0");
                        System.exit(-3);
                    }

                    break;
                default:
                    break;
            }

            if (messageStoreConfig.isEnableDLegerCommitLog()) {
                brokerConfig.setBrokerId(-1);
            }

            messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(brokerConfig.getRocketmqHome() + "/conf/logback_broker.xml");

            if (commandLine.hasOption('p')) { //yangyc -p 指定属性
                InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                MixAll.printObjectProperties(console, brokerConfig);
                MixAll.printObjectProperties(console, nettyServerConfig);
                MixAll.printObjectProperties(console, nettyClientConfig);
                MixAll.printObjectProperties(console, messageStoreConfig);
                System.exit(0);
            } else if (commandLine.hasOption('m')) {
                InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                MixAll.printObjectProperties(console, brokerConfig, true);
                MixAll.printObjectProperties(console, nettyServerConfig, true);
                MixAll.printObjectProperties(console, nettyClientConfig, true);
                MixAll.printObjectProperties(console, messageStoreConfig, true);
                System.exit(0);
            }

            log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
            MixAll.printObjectProperties(log, brokerConfig);
            MixAll.printObjectProperties(log, nettyServerConfig);
            MixAll.printObjectProperties(log, nettyClientConfig);
            MixAll.printObjectProperties(log, messageStoreConfig);

            //yangyc-main 创建 Controller
            final BrokerController controller = new BrokerController(
                brokerConfig,
                nettyServerConfig,
                nettyClientConfig,
                messageStoreConfig);
            // remember all configs to prevent discard
            controller.getConfiguration().registerConfig(properties);

            //yangyc-main 初始化 Controller
            // 1. 初始化网络通信组件（remotingServer---向nettyServer注册处理器、fastRemotingServer、各种线程池）
            // 2. 初始化业务功能组件
            //    1. messageStore 创建并加载分配策略、加载存储文件
            //    2. brokerOuterAPI 网络请求组件
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }

            //yangyc-main 服务关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;
                private AtomicInteger shutdownTimes = new AtomicInteger(0);

                @Override
                public void run() {
                    synchronized (this) {
                        log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                        if (!this.hasShutdown) {
                            this.hasShutdown = true;
                            long beginTime = System.currentTimeMillis();
                            controller.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                            log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                        }
                    }
                }
            }, "ShutdownHook"));

            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    private static void properties2SystemEnv(Properties properties) {
        if (properties == null) {
            return;
        }
        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain", MixAll.WS_DOMAIN_NAME);
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", MixAll.WS_DOMAIN_SUBGROUP);
        System.setProperty("rocketmq.namesrv.domain", rmqAddressServerDomain);
        System.setProperty("rocketmq.namesrv.domain.subgroup", rmqAddressServerSubGroup);
    }

    private static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}
