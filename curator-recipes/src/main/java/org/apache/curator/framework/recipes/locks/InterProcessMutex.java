/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.curator.framework.recipes.locks;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.curator.framework.CuratorFramework;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.utils.PathUtils;

/**
 * A re-entrant mutex that works across JVMs. Uses Zookeeper to hold the lock. All processes in all JVMs that
 * use the same lock path will achieve an inter-process critical section. Further, this mutex is
 * "fair" - each user will get the mutex in the order requested (from ZK's point of view)
 */
// 公平的可重入的锁
public class InterProcessMutex implements InterProcessLock, Revocable<InterProcessMutex>
{
    // 加锁释放锁具体类
    private final LockInternals internals;
    // 锁所在的目录，调用方传入
    private final String basePath;

    // 维护当前客户端每个线程的加锁状态
    private final ConcurrentMap<Thread, LockData> threadData = Maps.newConcurrentMap();

    private static class LockData
    {
        // 加锁线程
        final Thread owningThread;
        // 锁的节点路径
        final String lockPath;
        // 加锁数量，可重入的操作和标识
        final AtomicInteger lockCount = new AtomicInteger(1);

        private LockData(Thread owningThread, String lockPath)
        {
            this.owningThread = owningThread;
            this.lockPath = lockPath;
        }
    }

    // 锁名字前缀
    private static final String LOCK_NAME = "lock-";

    /**
     * @param client client
     * @param path   the path to lock
     */
    public InterProcessMutex(CuratorFramework client, String path)
    {
        this(client, path, new StandardLockInternalsDriver());
    }

    /**
     * @param client client
     * @param path   the path to lock
     * @param driver lock driver
     */
    public InterProcessMutex(CuratorFramework client, String path, LockInternalsDriver driver)
    {
        this(client, path, LOCK_NAME, 1, driver);
    }

    /**
     * Acquire the mutex - blocking until it's available. Note: the same thread
     * can call acquire re-entrantly. Each call to acquire must be balanced by a call
     * to {@link #release()}
     *
     * @throws Exception ZK errors, connection interruptions
     */
    // 加锁
    @Override
    public void acquire() throws Exception
    {
        if ( !internalLock(-1, null) )
        {
            throw new IOException("Lost connection while trying to acquire lock: " + basePath);
        }
    }

    /**
     * Acquire the mutex - blocks until it's available or the given time expires. Note: the same thread
     * can call acquire re-entrantly. Each call to acquire that returns true must be balanced by a call
     * to {@link #release()}
     *
     * @param time time to wait
     * @param unit time unit
     * @return true if the mutex was acquired, false if not
     * @throws Exception ZK errors, connection interruptions
     */
    // 支持超时时间的加锁
    @Override
    public boolean acquire(long time, TimeUnit unit) throws Exception
    {
        return internalLock(time, unit);
    }

    /**
     * Returns true if the mutex is acquired by a thread in this JVM
     *
     * @return true/false
     */
    @Override
    public boolean isAcquiredInThisProcess()
    {
        return (threadData.size() > 0);
    }

    /**
     * Perform one release of the mutex if the calling thread is the same thread that acquired it. If the
     * thread had made multiple calls to acquire, the mutex will still be held when this method returns.
     *
     * @throws Exception ZK errors, interruptions, current thread does not own the lock
     */
    // 释放锁
    @Override
    public void release() throws Exception
    {
        /*
            Note on concurrency: a given lockData instance
            can be only acted on by a single thread so locking isn't necessary
         */

        // 获取当前线程
        Thread currentThread = Thread.currentThread();
        // 获取线程对应的LockData
        LockData lockData = threadData.get(currentThread);
        if ( lockData == null )
        {
            // 为空，说明没有加过锁，直接抛出异常
            throw new IllegalMonitorStateException("You do not own the lock: " + basePath);
        }

        // 将lockData的计数器减1
        int newLockCount = lockData.lockCount.decrementAndGet();
        if ( newLockCount > 0 )
        {
            // 计数器还大于0，说明没有完全释放掉锁，可重入的，所以直接return
            return;
        }

        if ( newLockCount < 0 )
        {
            // 如果小于0，说明锁已经被释放了，直接抛出异常
            throw new IllegalMonitorStateException("Lock count has gone negative for lock: " + basePath);
        }
        try
        {
            // 走到这里，说明锁已经被完全释放了
            // 执行删除zk节点的操作
            internals.releaseLock(lockData.lockPath);
        }
        finally
        {
            // 说明174行执行成功，这里直接删除LockData
            threadData.remove(currentThread);
        }
    }

    /**
     * Return a sorted list of all current nodes participating in the lock
     *
     * @return list of nodes
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Collection<String> getParticipantNodes() throws Exception
    {
        return LockInternals.getParticipantNodes(internals.getClient(), basePath, internals.getLockName(), internals.getDriver());
    }

    @Override
    public void makeRevocable(RevocationListener<InterProcessMutex> listener)
    {
        makeRevocable(listener, MoreExecutors.directExecutor());
    }

    @Override
    public void makeRevocable(final RevocationListener<InterProcessMutex> listener, Executor executor)
    {
        internals.makeRevocable(new RevocationSpec(executor, new Runnable()
            {
                @Override
                public void run()
                {
                    listener.revocationRequested(InterProcessMutex.this);
                }
            }));
    }

    InterProcessMutex(CuratorFramework client, String path, String lockName, int maxLeases, LockInternalsDriver driver)
    {
        // 格式是：/ 或者/xxx/xx等
        basePath = PathUtils.validatePath(path);
        internals = new LockInternals(client, driver, path, lockName, maxLeases);
    }

    /**
     * Returns true if the mutex is acquired by the calling thread
     * 
     * @return true/false
     */
    // 判断当前线程是否持有锁
    public boolean isOwnedByCurrentThread()
    {
        LockData lockData = threadData.get(Thread.currentThread());
        return (lockData != null) && (lockData.lockCount.get() > 0);
    }

    protected byte[] getLockNodeBytes()
    {
        return null;
    }

    // 获取当前线程加锁的全路径名称
    protected String getLockPath()
    {
        LockData lockData = threadData.get(Thread.currentThread());
        return lockData != null ? lockData.lockPath : null;
    }

    // 具体的加锁逻辑
    private boolean internalLock(long time, TimeUnit unit) throws Exception
    {
        /*
           Note on concurrency: a given lockData instance
           can be only acted on by a single thread so locking isn't necessary
        */
        // 获取当前线程
        Thread currentThread = Thread.currentThread();
        // 获取当前线程加锁信息
        LockData lockData = threadData.get(currentThread);
        // 不为空，说明当前客户端当前线程加锁了
        if ( lockData != null )
        {
            // re-entering
            // 执行可重入操作即可，实现可重入加锁，即就在当前客户端的lockData的计数器自增
            lockData.lockCount.incrementAndGet();
            return true;
        }

        // 走到这，说明当前客户端当前线程还未加过锁
        // 获取加锁的节点路径
        // 参数：time 我们传入的超时时间
        // 参数：unit time的单位
        // 参数：getLockNodeBytes() 这里是null
        String lockPath = internals.attemptLock(time, unit, getLockNodeBytes());
        // 空判断，是空就直接返回false，加锁失败
        if ( lockPath != null )
        {
            // 创建加锁信息
            LockData newLockData = new LockData(currentThread, lockPath);
            // 放入threadData
            threadData.put(currentThread, newLockData);
            // 返回加锁成功
            return true;
        }

        // 返回加锁失败
        return false;
    }
}
