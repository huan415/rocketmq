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
package org.apache.rocketmq.namesrv;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;
import org.slf4j.LoggerFactory;

public class NamesrvStartup {

    private static InternalLogger log;
    private static Properties properties = null;
    private static CommandLine commandLine = null;

    public static void main(String[] args) {
        main0(args);
    }

    //yangyc-main
    // 1. 创建 NamesrvController。
    //    1. NamesrvConfig
    //    2. NettyServerConfig
    //    3. 创建 NamesrvController
    // 2. 启动 NamesrvController
    //    1. controller 初始化，1.初始化 remotingServer（重点：注册DefaultRequestProcessor）、2.routeInfoManager定时扫描检测 broker 状态、3.加载kv配置、 4.定时打印kv配置
    //    2. controller 启动 --- 启动 remotingServer
    //    3. 服务器关闭钩子.
    public static NamesrvController main0(String[] args) {

        try {
            NamesrvController controller = createNamesrvController(args); //yangyc-main 创建、初始化、启动 nameSrv
            start(controller);
            String tip = "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    //yangyc-main
    // 1. 创建 NamesrvController。
    //    1. NamesrvConfig
    //    2. NettyServerConfig
    //    3. 创建 NamesrvController
    public static NamesrvController createNamesrvController(String[] args) throws IOException, JoranException {
        //yangyc rocketmq 版本
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        //PackageConflictDetect.detectFastjson();

        Options options = ServerUtil.buildCommandlineOptions(new Options());
        //yangyc 启动时的参数信息, 由 commandLine 管理
        commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
            return null;
        }

        final NamesrvConfig namesrvConfig = new NamesrvConfig(); //yangyc namesrv 配置
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(9876); //yangyc namesrv 服务器监听端口修改为9876
        if (commandLine.hasOption('c')) { //yangyc -c 指定配置文件
            String file = commandLine.getOptionValue('c'); //yangyc 读取 -c 选项的值
            if (file != null) {
                InputStream in = new BufferedInputStream(new FileInputStream(file)); //yangyc 读取 config 文件数据到 properties 内
                properties = new Properties();
                properties.load(in);
                MixAll.properties2Object(properties, namesrvConfig); //yangyc 配置文件内的配置复写到 namesrvConfig
                MixAll.properties2Object(properties, nettyServerConfig); //yangyc 配置文件内的配置复写到 nettyServerConfig

                namesrvConfig.setConfigStorePath(file); //yangyc 将读取的配置文件路径设置到 namesrvConfig

                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }

        if (commandLine.hasOption('p')) { //yangyc -p 指定打印属性
            InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_NAME);
            MixAll.printObjectProperties(console, namesrvConfig);
            MixAll.printObjectProperties(console, nettyServerConfig);
            System.exit(0);
        }
        //yangyc 将启动时, 命令行设置的 kv 复写到 namesrvConfig 中
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);

        if (null == namesrvConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }
        //yangyc 创建日志对象
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(namesrvConfig.getRocketmqHome() + "/conf/logback_namesrv.xml");

        log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

        MixAll.printObjectProperties(log, namesrvConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);
        //yangyc 创建控制器, 参数1: namesrvConfig, 参数2: nettyServerConfig
        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig);

        // remember all configs to prevent discard
        controller.getConfiguration().registerConfig(properties);

        return controller;
    }

    //yangyc-main 启动 NamesrvController
    //    1. controller 初始化，1.初始化 remotingServer（重点：注册DefaultRequestProcessor）、2.routeInfoManager定时扫描检测 broker 状态、3.加载kv配置、 4.定时打印kv配置
    //    2. controller 启动 --- 启动 remotingServer
    //    3. 服务器关闭钩子.
    public static NamesrvController start(final NamesrvController controller) throws Exception {

        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }
        //yangyc 初始化。。。
        //yangyc-main 1.加载kv配置、2.初始化 remotingServer（重点：注册DefaultRequestProcessor）、3.定时扫 routeInfoManager 描检测 broker 状态 4.定时打印kv配置
        boolean initResult = controller.initialize();
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }
        //yangyc 服务关闭钩子（JVM HOOK）, 平滑关闭逻辑, 当 JVM 被关闭时, 主动调用 controller.shutdown(), 让服务器平滑关机。
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                controller.shutdown();
                return null;
            }
        }));
        //yangyc 启动服务器
        //yangyc-main 启动 remotingServer
        controller.start();

        return controller;
    }

    public static void shutdown(final NamesrvController controller) {
        controller.shutdown();
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static Properties getProperties() {
        return properties;
    }
}
