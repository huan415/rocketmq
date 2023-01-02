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
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.SelectMappedBufferResult;

public class HAConnection {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    //yangyc 上层对象
    private final HAService haService;
    //yangyc master 与 slave 之间会话通信的 SocketChannel
    private final SocketChannel socketChannel;
    //yangyc 客户端地址
    private final String clientAddr;
    //yangyc 写数据服务
    private WriteSocketService writeSocketService;
    //yangyc 读数据服务
    private ReadSocketService readSocketService;
    //yangyc 默认值：-1，它是在 slave 上报过 本地的消费进度之后 被赋值的。 它 >=0 了之后，同步数据的逻辑才会运行，为什么？
    // 因为 master 它不知道 slave 节点 当前消息存储进度在哪儿，它就没有办法去给 slave 推送数据。
    private volatile long slaveRequestOffset = -1;
    //yangyc 保存最新的 slave 上报的 offset 信息，slaveAckOffset 之前的数据，都可以认为 slave 已经全部同步完成了，对于的 ”生产者线程“ 需要被唤醒
    private volatile long slaveAckOffset = -1;

    public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
        this.haService = haService;
        this.socketChannel = socketChannel;
        this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);
        //yangyc 设置 socket 读写缓冲区为 64kb 大小
        this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
        this.socketChannel.socket().setSendBufferSize(1024 * 64);
        //yangyc 创建读写服务
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        this.readSocketService = new ReadSocketService(this.socketChannel);
        //yangyc 自增
        this.haService.getConnectionCount().incrementAndGet();
    }

    public void start() {
        this.readSocketService.start();
        this.writeSocketService.start();
    }

    public void shutdown() {
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    class ReadSocketService extends ServiceThread {
        //yangyc 1 mb
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        //yangyc 多路复用器
        private final Selector selector;
        //yangyc master 与 slave 之间的会话 socketChannel
        private final SocketChannel socketChannel;
        //yangyc slave 向 master 传输的帧格式：
        // [long][long][long][long][long][long]...
        // slave 向 master 上报的是 slave 本地的同步进度，这个同步进度是一个 long 值。
        //yangyc 读取缓冲区，1 mb
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        //yangyc 缓冲区处理位点
        private int processPosition = 0;
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            //yangyc socketChannel 注册到多路复用器，关注 ”OP_READ“ 事件
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    //yangyc 多路复用器阻塞，最长 1s
                    this.selector.select(1000);
                    //yangyc 两种情况：1.事件就绪（可读事件）；2.阻塞超时
                    //yangyc-main 处理读事件
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }

                    long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        //yangyc 长时间未发生通信的话，退出循环，结束 HAConnection 连接
                        log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }
            //yangyc 设置 serviceThread 状态为 stopped
            this.makeStop();
            //yangyc 将读服务 对应的 写服务 也设置线程状态为 stopped
            writeSocketService.makeStop();
            //yangyc 移除当前 HAConnection 对象， 从 haService
            haService.removeConnection(HAConnection.this);
            //yangyc 减一
            HAConnection.this.haService.getConnectionCount().decrementAndGet();

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                //yangyc socket 关闭
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }
        //yangyc 处理读事件。 返回值boolean： true:正常，读取成功。 false:socket 半关闭状态，需要上层重建当前 HAConnection 对象
        private boolean processReadEvent() {
            //yangyc 循环控制变量，当连续从 socket 读取失败 3 次（未加载到数据），那么跳出循环
            int readSizeZeroTimes = 0;
            //yangyc 条件成立 说明 byteBufferRead 已经全部使用完了，没有剩余的空间了
            if (!this.byteBufferRead.hasRemaining()) {
                //yangyc 相当于清理操作，其实将 pos=0
                this.byteBufferRead.flip();
                //yangyc 处理位点信息 归 0
                this.processPosition = 0;
            }

            while (this.byteBufferRead.hasRemaining()) {
                try {
                    //yangyc 到 socket 读缓冲区加载数据， readSize 表示加载的数据量
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) { //yangyc CASE1: 加载成功
                        readSizeZeroTimes = 0;
                        this.lastReadTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                        //yangyc 条件成立： 说明 byteBufferRead 中可读数据最少包含一个数据帧
                        if ((this.byteBufferRead.position() - this.processPosition) >= 8) {
                            //yangyc pos 表示 byteBufferRead 可读数据帧中，最后一个帧数据（前面的帧不要了）
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);
                            //yangyc 读取最后一帧数据，slave 端当前的同步进度信息
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            //yangyc 更新处理位点
                            this.processPosition = pos;
                            //yangyc 赋值
                            HAConnection.this.slaveAckOffset = readOffset;
                            //yangyc 条件成立：slaveRequestOffset == -1， 这个时候是给 slaveRequest 赋值的逻辑，slaveRequestOffset 在写数据服务要使用的
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }
                            //yangyc 唤醒阻塞的”生产者线程“
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) { //yangyc CASE2: 加载失败，读缓冲区没有数据可加载
                        if (++readSizeZeroTimes >= 3) {
                            //yangyc 一般懂这里跳出循环
                            break;
                        }
                    } else { //yangyc CASE3: socket 半关闭状态，需要上层关闭当前 HAConnection 连接对象,
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }

    class WriteSocketService extends ServiceThread {
        //yangyc 多路复用器
        private final Selector selector;
        //yangyc master 和 slave 之间的会话 socketChannel
        private final SocketChannel socketChannel;
        //yangyc 协议头大小：12
        private final int headerSize = 8 + 4;
        //yangyc master 和 slave 传输的数据格式：
        // {[phyOffset][size][data...][phyOffset][size][data...][phyOffset][size][data...]}
        // phyOffset: 数据区间的开始偏移量，表示的是数据快开始的偏移量，并不表示一条具体的消息
        // size: 同步的数据快的大小
        // data: 数据快，最大 32k, 可能包含多条消息的数据。

        //yangyc 帧头的缓冲区
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(headerSize);
        //yangyc 下一次传输同步数据的位置信息， 非常重要（master 需要知道给当前slave同步的位点）
        private long nextTransferFromWhere = -1;
        //yangyc mappedFile 的查询封装对象，内部有 byteBuffer
        private SelectMappedBufferResult selectMappedBufferResult;
        //yangyc 上一轮数据是否传输完毕
        private boolean lastWriteOver = true;
        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            //yangyc socketChannel 注册到多路复用器。关注”OP_WRITE“事件
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    //yangyc 多路复用器阻塞，最长 1s
                    this.selector.select(1000);
                    //yangyc 两种情况：1.socket写缓冲区有空间可写了； 2.阻塞超时
                    if (-1 == HAConnection.this.slaveRequestOffset) {
                        //yangyc 休眠一段事件
                        Thread.sleep(10);
                        continue;
                    }

                    if (-1 == this.nextTransferFromWhere) {
                        //yangyc 初始化 nextTransferFromWhere 的逻辑。。。
                        if (0 == HAConnection.this.slaveRequestOffset) {
                            //yangyc slave 是一个新节点，走这里赋值
                            //yangyc 从最后一个正在顺序写的 mappedFile 开始同步数据
                            //yangyc 获取 master 最大的 offset
                            long masterOffset = HAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                            //yangyc 计算 maxOffset 归属的 mappedFile 文件的开始 offset
                            masterOffset =
                                masterOffset
                                    - (masterOffset % HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                    .getMappedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            this.nextTransferFromWhere = masterOffset;
                        } else {
                            //yangyc 一般从这里赋值
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + HAConnection.this.clientAddr
                            + "], and slave request " + HAConnection.this.slaveRequestOffset);
                    }

                    if (this.lastWriteOver) { //yangyc 上一轮待发生数据全部发生完成

                        long interval =
                            HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;

                        if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaSendHeartbeatInterval()) {
                            //yangyc 发送一个 header 数据包，当作心跳，维持长连接
                            // Build Header
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(headerSize);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();
                            //yangyc lastWriteOver 表示上一轮是否处理完成
                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }
                    } else { //yangyc 上一轮的待发送数据未全部发送
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }
                    //yangyc 到 commitLog 中查询 nextTransferFromWhere 开始位置的数据，返回 smbr 对象
                    SelectMappedBufferResult selectResult =
                        HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
                    if (selectResult != null) {
                        //yangyc size 有可能很大，
                        int size = selectResult.getSize();
                        if (size > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            //yangyc 将 size 设置为 32K. 即：最大32k
                            size = HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }
                        //yangyc 构建 header 使用
                        long thisOffset = this.nextTransferFromWhere;
                        //yangyc 增加 size, 下一轮传输跳过本帧数据
                        this.nextTransferFromWhere += size;
                        //yangyc 设置 byteBuffer 可访问数据区间为 [pos, size]
                        selectResult.getByteBuffer().limit(size);
                        //yangyc 赋值给写数据服务的 selectMappedBufferResult 对象
                        this.selectMappedBufferResult = selectResult;

                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(headerSize);
                        this.byteBufferHeader.putLong(thisOffset);
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();

                        this.lastWriteOver = this.transferData();
                    } else {

                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {

                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            HAConnection.this.haService.getWaitNotifyObject().removeFromWaitingThreadTable();

            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }

            this.makeStop();

            readSocketService.makeStop();

            haService.removeConnection(HAConnection.this);

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }
        //yangyc 同步数据到 slave 节点。返回值boolean:  true:表示本轮数据全部同步完成（header+smbr）  false:表示本轮同步未完成（header 或者 smbr 其中一个未同步完成都会返回 false）
        private boolean transferData() throws Exception {
            //yangyc 循环控制变量，当写失败3次时，跳出循环
            int writeSizeZeroTimes = 0;
            //yangyc 帧头数据 发送操作
            // Write Header
            while (this.byteBufferHeader.hasRemaining()) { //yangyc 条件成立：说明 byteBufferHeader 有待读取的数据
                //yangyc 向 socket写缓冲区写数据。 writeSize：写成功的数据量
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) { //yangyc CASE1:写成功
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) { //yangyc CASE1:写失败
                    if (++writeSizeZeroTimes >= 3) {
                        //yangyc 一般从这里跳出循环
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }
            //yangyc selectMappedBufferResult 保存的是本轮待同步给 slave 的数据
            if (null == this.selectMappedBufferResult) { //yangyc 条件成立：心跳包
                //yangyc 判断心跳包有没有全部发送完成。。。
                return !this.byteBufferHeader.hasRemaining();
            }
            //yangyc 循环变量归 0
            writeSizeZeroTimes = 0;

            // Write Body
            if (!this.byteBufferHeader.hasRemaining()) { //yangyc 只有 Header 写成功之后，才进行写 body 逻辑
                //yangyc 循环条件：hasRemaining() 返回 true。 表示 smbr 内有待处理数据， 正常情况在 while 条件内结束循环
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    //yangyc 向 socket 写缓冲区写数据， writeSize 表示本次写的字节数
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) { //yangyc CASE1: 写成功，（不代表 selectMappedBufferResult 中的数据全部写完成）
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) { //yangyc CASE2: 写失败，因为 socket 写缓冲区写满了
                        if (++writeSizeZeroTimes >= 3) {
                            //yangyc 跳出
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }
            //yangyc 返回值boolean:  true:表示本轮数据全部同步完成（header+smbr）  false:表示本轮同步未完成（header 或者 smbr 其中一个未同步完成都会返回 false）
            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();
            //yangyc 条件成立：说明本轮 smbr 内的数据全部发送完成了。。
            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                //yangyc 释放
                this.selectMappedBufferResult.release();
                //yangyc 设置为 null
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }
}
