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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4; //yangyc 每个 hash 桶的大小：4byte
    private static int indexSize = 20; //yangyc 每个index条目的大小：20byte
    private static int invalidIndex = 0; //yangyc 无效索引编号：0
    private final int hashSlotNum; //yangyc hash槽数，默认：500w
    private final int indexNum; //yangyc  索引数量，默认：2000w
    private final MappedFile mappedFile; //yangyc 索引文件使用的mf
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer; //yangyc 从 mf 中获取的内存映射缓冲区
    private final IndexHeader indexHeader; //yangyc 索引头对象
    //yangyc 参数endPhyOffset：上一个索引文件最后一条消息的物理偏移量，参数endTimestamp：上一个索引文件最后一条消息的存储时间
    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize); //yangyc 计算文件大小：40 + 500w*4 + 2000w*20
        this.mappedFile = new MappedFile(fileName, fileTotalSize); //yangyc 创建 mf 对象，会在 disk 上创建文件
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer); //yangyc 创建索引头对象，传递索引文件 mf 的索引切片数据

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    //yangyc-main indexHeader 依次从 byteBuffer 中读取 header 数据, 赋值到对象字段
    public void load() {
        this.indexHeader.load();
    }

    //yangyc-main 将 mappedByteBuffer 内的数据落盘
    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    //yangyc-main 检查当前 indexFile 已写索引数 是否 >= indexNum。 达到该值则当前 IndexFile 可不再追加 indexDatav 了。
    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    //yangyc-main 删除文件, 调用 mappedFile.destory() 方法, 删除索引文件删除的方法。
    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    //yangyc-main 添加索引数据
    //yangyc 参数1：1.uniq_key  2.keys="aa bb cc" 会分别为aa、bb、cc 创建索引，参数2：消息物理偏移量，参数3：消息存储时间
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) { //yangyc 条件成立：说明索引文件还有存储时间
            //yangyc-main absSlotPos = 40 + sloPos*4
            int keyHash = indexKeyHashMethod(key); //yangyc 获取 key 的hash 值，这个值是正数
            int slotPos = keyHash % this.hashSlotNum; //yangyc 取模。获取key对应hash桶的下标
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize; //yangyc 根据sloPos计算出keyhash桶的开始位置

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos); //yangyc-main 读取hash桶内的原值（当hash冲突时才有值，其他情况slotValues的invalidIndex=0）
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) { //yangyc-main 条件成立：说明 slotValue 是一个无效值
                    slotValue = invalidIndex; //yangyc 先把 hash 槽内的原值保持到临时变量
                }
                //yangyc-main 计算事件差值：当前 msg 存储时间 - 索引文件第一条消息的存储时间，得到一个差值（好处：差值使用4byte,相对于storeTimestamp需要8byte,节省了4byte）
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000; //yangyc 转换成秒
                //yangyc 第一条索引插入时，timeDiff=0
                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }
                //yangyc-main 计算索引存储的开始位置：40 + 500w*4 + 索引编号*20
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;

                //yangyc-main 填充索引字段
                this.mappedByteBuffer.putInt(absIndexPos, keyHash); //yangyc key hashCode（4字节）
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset); //yangyc 消息物理偏移量（8字节）
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff); //yangyc 消息存储时间（第一条索引条目的差值）（4字节）
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue); //yangyc hash 桶的原值（当hash冲突时，会使用到）

                //yangyc-main 更新 hash 槽内的值, 设置为 IndexCount
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount()); //yangyc 向当前key计算出来的hash桶内写入的索引编号

                //yangyc-main 更新索引头
                if (this.indexHeader.getIndexCount() <= 1) { //yangyc 索引文件存储的如果是第一条 indexData
                    this.indexHeader.setBeginPhyOffset(phyOffset); //yangyc 更新索引头 beginPhyOffset
                    this.indexHeader.setBeginTimestamp(storeTimestamp); //yangyc 更新索引头 beginTimestamp
                }

                if (invalidIndex == slotValue) { //yangyc hash 槽未冲突时, 自增索引头
                    this.indexHeader.incHashSlotCount(); //yangyc 占用 hash 桶数量加一
                }
                this.indexHeader.incIndexCount(); //yangyc 索引编号自增，索引条目加一
                this.indexHeader.setEndPhyOffset(phyOffset); //yangyc 设置最后一条消息物理偏移量 到索引头
                this.indexHeader.setEndTimestamp(storeTimestamp); //yangyc 设置最后一条消息存储时间 到索引头

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    //yangyc-main 从索引文件查询消息的物理偏移量, 结果由 phyOffsets 保存
    //yangyc 参数1：查询结果全部放到该list，参数2：查询key，参数3：结果最大数限制 。。。
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) { //yangyc 引用计数加一，避免查询期间，mf 资源被释放
            //yangyc-main absSlotPos = 40 + sloPos*4
            int keyHash = indexKeyHashMethod(key); //yangyc 获取当前 key hash 值
            int slotPos = keyHash % this.hashSlotNum; //yangyc 计算出 key hash 对应的 hash 桶下标值
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize; //yangyc 计算出 hash 桶存储的开始位置：40+下标值*4

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                //yangyc-main 根据 absSlotPos 读取出 hash 槽内存储的 “索引编号”
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos); //yangyc 获取出 hash 桶内的值，这个值可能是无效值、也有可能是索引编号
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) { //yangyc 条件成立：hash 未命中
                } else { //yangyc 正常走这里
                    for (int nextIndexToRead = slotValue; ; ) { //yangyc 下一条要读取的索引编号
                        if (phyOffsets.size() >= maxNum) { //yangyc 停止查询条件
                            break;
                        }
                        //yangyc-main 根据索引编号计算出索引数据的开始位置
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;
                        //yangyc-main 读取索引数据: 1.keyHashRead  2.phyOffsetRead  3.timeDiff  4.prevIndexRead (hash 冲突时才有值)
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }
                        //yangyc 转换成毫秒
                        timeDiff *= 1000L;
                        //yangyc-main 计算出 msg 准确的存储时间
                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        //yangyc-main 时间范围匹配。检查 timeRead 是否在 begin 和 end 区间, 在的话将 phyOffsetRead 保持到结果 list 内（phyOffsets）
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);
                        //yangyc 条件成立：说明查询命中，将消息索引的 消息偏移量 加入到list集合中
                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }
                        //yangyc-main 检查 prevIndexRead 是否有效, 有效则循环, 无效则跳出
                        //yangyc 判断索引条目的 前驱索引编号 是否是无效值.  无效则跳出查询
                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }
                        //yangyc prevIndexRead有效，赋值给 nextIndexToRead 继续向前查询，解决hash冲突
                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release(); //yangyc 引用计数减一
            }
        }
    }
}
