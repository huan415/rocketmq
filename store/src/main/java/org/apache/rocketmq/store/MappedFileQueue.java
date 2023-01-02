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
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class MappedFileQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private static final int DELETE_FILES_BATCH_MAX = 10;
    //yangyc mfq 管理的目录（CommitLog: ../store/commitlog  或者 consumeQueue:../store/xxx_topic/0）
    private final String storePath;
    //yangyc 目录下每个文件大小（commitLog: 默认1g  或者   consumeQueue: 600w 字节）
    private final int mappedFileSize;
    //yangyc 目录下每个 mappedFile 集合
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();
    //yangyc 创建 mappedFile 的服务。 向它提交 request，会创建并返回一个 mappedFile 对象
    private final AllocateMappedFileService allocateMappedFileService;

    private long flushedWhere = 0; //yangyc 目录下的刷盘位点（值：curMappedFile.fileName + curMappedFile.wrotePosition）
    private long committedWhere = 0;

    private volatile long storeTimestamp = 0; //yangyc 当前目录下最后一条 msg 的存储时间

    public MappedFileQueue(final String storePath, int mappedFileSize,
        AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    public void checkSelf() {

        if (!this.mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();

                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                            pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    public MappedFile getMappedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return null;

        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        return (MappedFile) mfs[mfs.length - 1];
    }

    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;

        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();

        for (MappedFile file : this.mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }

        this.deleteExpiredFile(willRemoveFiles);
    }

    public boolean load() {
        File dir = new File(this.storePath); //yangyc 创建目录对象
        File[] files = dir.listFiles(); //yangyc 获取目录下所有的文件
        if (files != null) {
            // ascending order
            Arrays.sort(files); //yangyc 按照文件名排序
            for (File file : files) {

                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length()
                        + " length not matched message store config value, please check it manually");
                    return false;
                }

                try {
                    //yangyc 为当前的 file 创建对应的 mappedFile 对象
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);
                    //yangyc 设置 wrotePosition、flushedPosition、commitedPosition 值为 mappedFileSize, 并不是准确值，准确值在 recover 阶段设置
                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
                    mappedFile.setCommittedPosition(this.mappedFileSize);
                    //yangyc 将当前 file 的 mappedFile 对象加入到 list 集合管理
                    this.mappedFiles.add(mappedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }

    void deleteExpiredFile(List<MappedFile> files) {

        if (!files.isEmpty()) {

            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }
    //yangyc 参数1：startOffset 文件起始偏移量。  参数2：当文件不存在时, 是否需要创建 mappedFile
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        //yangyc 该值控制是否创建 mappedFile, 当需要创建 mappedFile时，它充当创建文件名的结尾
        //yangyc 两种情况会创建：情况1.list 内没有 mappedFile    情况2.list 最后一个 mappedFile（当前顺序写的mappedFile）已经写满了
        long createOffset = -1;
        //yangyc-main 获取当前正则顺序写的 MappedFile
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast == null) { //yangyc 情况1.list 内没有 mappedFile
            createOffset = startOffset - (startOffset % this.mappedFileSize); //yangyc createOffset 的取值必须是 mappedFileSize 的倍数或者 0
        }

        if (mappedFileLast != null && mappedFileLast.isFull()) { //yangyc 情况2.list 最后一个 mappedFile（当前顺序写的mappedFile）已经写满了
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize; //yangyc 上一个文件名转long + mappedFileSize
        }

        if (createOffset != -1 && needCreate) { //yangyc 条件成立：需要创建新的 mappedFile
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset); //yangyc 获取待创建文件的绝对路径（下次即将创建的文件名）
            String nextNextFilePath = this.storePath + File.separator
                + UtilAll.offset2FileName(createOffset + this.mappedFileSize); //yangyc 获取下下次要创建文件的绝对路径
            MappedFile mappedFile = null;

            if (this.allocateMappedFileService != null) {
                //yangyc-main 创建 mappedFile 的服务。 向它提交 request，会创建并返回一个 mappedFile 对象
                //yangyc 当 mappedFileSize >= 1g 的时候，这里创建 mappedFile 之后会执行预热方法
                mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                    nextNextFilePath, this.mappedFileSize);
            } else {
                try {
                    mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                } catch (IOException e) {
                    log.error("create mappedFile exception", e);
                }
            }

            if (mappedFile != null) {
                if (this.mappedFiles.isEmpty()) {
                    mappedFile.setFirstCreateInQueue(true);
                }
                this.mappedFiles.add(mappedFile);
            }

            return mappedFile;
        }

        return mappedFileLast;
    }

    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;

        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }

        return mappedFileLast;
    }

    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getFileFromOffset() +
                mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;

            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff)
                return false;
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();

        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }
    //yangyc-main 获取 MappedFileQueue 管理的最小物理偏移量，其实就是 list(0) 这个文件名称表示的偏移量
    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    //yangyc-main 获取 MappedFileQueue 管理的最大物理偏移量，其实就是 当前顺序写的 MappedFile 文件名 + MappedFile.wrotePosition
    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            //yangyc 文件名 + 文件正在顺序写的数据位点 => maxOffset
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }

    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }
    //yangyc-main commitedLog 目录删除过期文件，根据文件”保留时长“删除过期文件
    //yangyc 参数1:过期时间, 参数2:删除两个文件之间的时间间隔, 参数3:mappedFile.destroy的参数, 参数4:是否强制删除（不考虑expiredTime这个条件）
    public int deleteExpiredFileByTime(final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately) {
        Object[] mfs = this.copyMappedFiles(0); //yangyc 获取 mfs 数组

        if (null == mfs)
            return 0;

        int mfsLength = mfs.length - 1;
        int deleteCount = 0; //yangyc 删除的文件数
        List<MappedFile> files = new ArrayList<MappedFile>(); //yangyc 被删除的问价列表
        if (null != mfs) {
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime; //yangyc 计算出当前文件存活时间的截止点
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) { //yangyc 条件成立：1. 文件存活时间达到上线 或者 2：disk 占用率达到上线会走强制删除
                    //yangyc-main 删除 mappedFile
                    if (mappedFile.destroy(intervalForcibly)) {
                        files.add(mappedFile);
                        deleteCount++;

                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
                            break;
                        }

                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    //avoid deleting files in the middle
                    break;
                }
            }
        }

        deleteExpiredFile(files); //yangyc 将删除文件的 mf 从 queue 内删除

        return deleteCount;  //yangyc 返回删除的文件数
    }
    //yangyc-main consumerQueue 目录删除过期文件。参数1：commitLog目录下的最小物理偏移量（第一条消息 offset）；参数2：consumeQueue 文件内每个数据单元固定大小；
    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {
            //yangyc 这里减一，是保证当前正在顺序写的 mf 不被删除
            int mfsLength = mfs.length - 1;
            //yangyc-main 遍历每个 mappedFile, 读取最后一条数据，提取出 CQData->msgPhyOffset 值，如果这个值小于 offset，则删除该 mappedFile 文件
            for (int i = 0; i < mfsLength; i++) {
                boolean destroy; //yangyc 当前 mf 是否删除
                MappedFile mappedFile = (MappedFile) mfs[i];
                //yangyc 获取当前文件最后一个数据单元
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    //yangyc 获取 cqData.msgPhyOffset
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    //yangyc 最终调用 mappedFile.release()
                    result.release();
                    destroy = maxOffsetInLogicQueue < offset;//yangyc true：说明当前 mf 内的全部的 cqData 都是过期数据
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                            + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files); //yangyc 将删除文件的 mf 从 queue 内删除

        return deleteCount; //yangyc 返回删除的文件数
    }
    //yangyc-main 根据 flushedWhere 查找合适的 MappedFile 对象，并调用该 MappedFile 的落盘方法，并更新全局 flushedWhere 值
    //yangyc 返回值：true:表示本次刷盘无数据落盘；false:表示本次刷盘有数据落盘。 参数：flushLeastPages (0：表示强制刷新； >0: 脏页数据必须达到flushLeastPages才刷新)
    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        //yangyc-main 获取当前正在刷盘的文件（正在顺序写的 mappedFile）
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            long tmpTimeStamp = mappedFile.getStoreTimestamp();//yangyc 获取mappedFile最后一条消息的存储时间
            int offset = mappedFile.flush(flushLeastPages);//yangyc-main 调用 mappedFile 去刷盘。返回最新的落盘位点。
            long where = mappedFile.getFileFromOffset() + offset; //yangyc 起始偏移量 + 最新落盘位点
            result = where == this.flushedWhere; //yangyc true:表示本次刷盘无数据落盘；false:表示本次刷盘有数据落盘
            this.flushedWhere = where; //yangyc 将最新的目录刷盘位点赋值给 flushedWhere
            if (0 == flushLeastPages) { //yangyc 强制刷盘
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            int offset = mappedFile.commit(commitLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.committedWhere;
            this.committedWhere = where;
        }

        return result;
    }

    /**
     * Finds a mapped file by offset.
     *
     * @param offset Offset.
     * @param returnFirstOnNotFound If the mapped file is not found, then return the first one.
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    //yangyc-main 根据 offset 查找偏移量区间包含该 offset 的 mappedFile
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            MappedFile firstMappedFile = this.getFirstMappedFile(); //yangyc 第一个 mappedFile
            MappedFile lastMappedFile = this.getLastMappedFile(); //yangyc 最后一个 mappedFile
            if (firstMappedFile != null && lastMappedFile != null) {
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    //yangyc 条件成立：说明 offset 没能命中 list 内的 mappedFile
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                        offset,
                        firstMappedFile.getFileFromOffset(),
                        lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                        this.mappedFileSize,
                        this.mappedFiles.size());
                } else { //yangyc 正常走这里
                    //yangyc 比如说：commitLog目录有文件：5g、6g、7g、8g、9g。我们要查找offset包含7.6g的mappedFile
                    //yangyc 计算 mappedFiles 数组下标；  (7.6/1 - 5/1)=2 ==> index=2
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }

                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                        && offset < targetFile.getFileFromOffset() + this.mappedFileSize) { //yangyc 条件成立：说明 mappedFile 内包含 offset 偏移量
                        return targetFile; //yangyc 正常从这里返回
                    }

                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                            && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }

                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}
