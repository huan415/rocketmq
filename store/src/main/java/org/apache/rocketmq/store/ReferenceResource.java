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

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {
    protected final AtomicLong refCount = new AtomicLong(1); //yangyc refCount 引用数量。当引用数量<=0的时候, 表示该资源可以释放了, 没有任何其他程序依赖它了
    protected volatile boolean available = true; //yangyc 是否存活，默认是 true。当 available=false, 表示资源非存活状态，不可用
    protected volatile boolean cleanupOver = false; //yangyc 是否已经清理, 默认是 false。当执行完子类对象的 cleanup()后, 该值设置为 true。表示资源已经全部释放了
    //yangyc 第一次关闭资源的时间（因为第一次关闭资源可能会失败--比如说外部程序还依赖当前资源，refCount>0, 此时在这记录初次关闭资源的时间，
    // 当之后再次关闭资源的时候，会传递一个 interval 参数，如果系统当前时间-firstShutdownTimestamp>interval, 则执行强制关闭）
    private volatile long firstShutdownTimestamp = 0;
    //yangyc 增加引用计数。返回 true:添加成功； false:添加失败
    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }
    //yangyc 关闭资源，参数：强制关闭的时间间隔
    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();//yangyc 保存初次关闭时的系统时间
            this.release(); //yangyc 引用计数-1（有可能释放了资源，也有可能没有释放）
        } else if (this.getRefCount() > 0) {
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount()); //yangyc 强制设置引用计数为负数
                this.release();//yangyc 引用计数-1（一定会释放了资源）
            }
        }
    }
    //yangyc 引用计数-1
    public void release() {
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;
        //yangyc 执行到这里，说明当前资源无其他程序依赖了，可以调用 cleanup 释放真正的资源了
        synchronized (this) {

            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
