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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.curator.RetryLoop;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.PathAndBytesable;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * A counting semaphore that works across JVMs. All processes
 * in all JVMs that use the same lock path will achieve an inter-process limited set of leases.
 * Further, this semaphore is mostly "fair" - each user will get a lease in the order requested
 * (from ZK's point of view).
 * </p>
 * <p>
 * There are two modes for determining the max leases for the semaphore. In the first mode the
 * max leases is a convention maintained by the users of a given path. In the second mode a
 * {@link SharedCountReader} is used as the method for semaphores of a given path to determine
 * the max leases.
 * </p>
 * <p>
 * If a {@link SharedCountReader} is <b>not</b> used, no internal checks are done to prevent
 * Process A acting as if there are 10 leases and Process B acting as if there are 20. Therefore,
 * make sure that all instances in all processes use the same numberOfLeases value.
 * </p>
 * <p>
 * The various acquire methods return {@link Lease} objects that represent acquired leases. Clients
 * must take care to close lease objects  (ideally in a <code>finally</code>
 * block) else the lease will be lost. However, if the client session drops (crash, etc.),
 * any leases held by the client are automatically closed and made available to other clients.
 * </p>
 * <p>
 * Thanks to Ben Bangert (ben@groovie.org) for the algorithm used.
 * </p>
 */
// 基于zk实现的信号量
// 如果一个客户端尝试去获取semaphore的锁，直接核心的逻辑，就是尝试获取自动生成的一个锁：/semaphores/semaphore_01/locks，底层就是基于顺序节点来排队获取锁的，用的是我们之前给大家讲解的那个锁的原理
//这个普通的锁同一时间只能有一个客户端获取到这个锁，其他的客户端都会通过顺序节点排队等候
//PathAndBytesable<String> createBuilder = client.create().creatingParentsIfNeeded().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL);
// /semaphores/semaphore_01/leases/_c_a9302e20-de9c-4356-923a-274664d7676c-lease-0000000000
//在lease path下，创建了一个顺序节点
//创建了一个Lease，这个Lease对象其实就代表了客户端A成功加的一个semaphore的这个锁，他先尝试成功加锁，如果成功加锁了以后，他就可以创建一个这个锁对应的一个lease，比如semaphore一共可以允许最多3个客户端同时加锁
//最多是允许不超过3个客户端获取锁，他在这里会判断一下逻辑，就是说当前/semaphores/semaphore_01/leases目录下的节点的数量是否<=3

// 在释放semaphore锁的时候，会调用Lease的close()方法
// 内部借助了一个普通的锁，保证说所有的客户端都是按照顺序在尝试加semaphore锁的

// InterProcessMutex锁的path是即basePath：/semaphores/semaphore_01/locks
// leasePath：/semaphores/semaphore_01/leases
public class InterProcessSemaphoreV2
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    // InterProcessMutex锁
    private final InterProcessMutex lock;
    private final WatcherRemoveCuratorFramework client;
    private final String leasesPath;
    // 监听器
    private final Watcher watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            client.postSafeNotify(InterProcessSemaphoreV2.this);
        }
    };

    private volatile byte[] nodeData;
    // 最大允许的加锁线程
    private volatile int maxLeases;

    private static final String LOCK_PARENT = "locks";
    private static final String LEASE_PARENT = "leases";
    private static final String LEASE_BASE_NAME = "lease-";
    public static final Set<String> LOCK_SCHEMA = Sets.newHashSet(
            LOCK_PARENT,
            LEASE_PARENT
    );

    /**
     * @param client    the client
     * @param path      path for the semaphore
     * @param maxLeases the max number of leases to allow for this instance
     */
    public InterProcessSemaphoreV2(CuratorFramework client, String path, int maxLeases)
    {
        this(client, path, maxLeases, null);
    }

    /**
     * @param client the client
     * @param path   path for the semaphore
     * @param count  the shared count to use for the max leases
     */
    public InterProcessSemaphoreV2(CuratorFramework client, String path, SharedCountReader count)
    {
        this(client, path, 0, count);
    }

    private InterProcessSemaphoreV2(CuratorFramework client, String path, int maxLeases, SharedCountReader count)
    {
        this.client = client.newWatcherRemoveCuratorFramework();
        path = PathUtils.validatePath(path);
        // 创建InterProcessMutex锁，basePath是locks
        lock = new InterProcessMutex(client, ZKPaths.makePath(path, LOCK_PARENT));
        // 设置信号量允许被获取所得客户端数量
        this.maxLeases = (count != null) ? count.getCount() : maxLeases;
        // 设置获取信号量的客户端加锁basePath即leases
        leasesPath = ZKPaths.makePath(path, LEASE_PARENT);

        if ( count != null )
        {
            count.addListener
                (
                    new SharedCountListener()
                    {
                        @Override
                        public void countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception
                        {
                            InterProcessSemaphoreV2.this.maxLeases = newCount;
                            client.postSafeNotify(InterProcessSemaphoreV2.this);
                        }

                        @Override
                        public void stateChanged(CuratorFramework client, ConnectionState newState)
                        {
                            // no need to handle this here - clients should set their own connection state listener
                        }
                    }
                );
        }
    }

    /**
     * Set the data to put for the node created by this semaphore. This must be called prior to calling one
     * of the acquire() methods.
     *
     * @param nodeData node data
     */
    public void setNodeData(byte[] nodeData)
    {
        this.nodeData = (nodeData != null) ? Arrays.copyOf(nodeData, nodeData.length) : null;
    }

    /**
     * Return a list of all current nodes participating in the semaphore
     *
     * @return list of nodes
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Collection<String> getParticipantNodes() throws Exception
    {
        return client.getChildren().forPath(leasesPath);
    }

    /**
     * Convenience method. Closes all leases in the given collection of leases
     *
     * @param leases leases to close
     */
    public void returnAll(Collection<Lease> leases)
    {
        for ( Lease l : leases )
        {
            CloseableUtils.closeQuietly(l);
        }
    }

    /**
     * Convenience method. Closes the lease
     *
     * @param lease lease to close
     */
    public void returnLease(Lease lease)
    {
        CloseableUtils.closeQuietly(lease);
    }

    /**
     * <p>Acquire a lease. If no leases are available, this method blocks until either the maximum
     * number of leases is increased or another client/process closes a lease.</p>
     * <p>The client must close the lease when it is done with it. You should do this in a
     * <code>finally</code> block.</p>
     *
     * @return the new lease
     * @throws Exception ZK errors, interruptions, etc.
     */
    // 获取信号量锁
    public Lease acquire() throws Exception
    {
        Collection<Lease> leases = acquire(1, 0, null);
        return leases.iterator().next();
    }

    /**
     * <p>Acquire <code>qty</code> leases. If there are not enough leases available, this method
     * blocks until either the maximum number of leases is increased enough or other clients/processes
     * close enough leases.</p>
     * <p>The client must close the leases when it is done with them. You should do this in a
     * <code>finally</code> block. NOTE: You can use {@link #returnAll(Collection)} for this.</p>
     *
     * @param qty number of leases to acquire
     * @return the new leases
     * @throws Exception ZK errors, interruptions, etc.
     */
    // 获取信号量锁
    public Collection<Lease> acquire(int qty) throws Exception
    {
        return acquire(qty, 0, null);
    }

    /**
     * <p>Acquire a lease. If no leases are available, this method blocks until either the maximum
     * number of leases is increased or another client/process closes a lease. However, this method
     * will only block to a maximum of the time parameters given.</p>
     * <p>The client must close the lease when it is done with it. You should do this in a
     * <code>finally</code> block.</p>
     *
     * @param time time to wait
     * @param unit time unit
     * @return the new lease or null if time ran out
     * @throws Exception ZK errors, interruptions, etc.
     */
    // 待超时时间的获取信号量锁
    public Lease acquire(long time, TimeUnit unit) throws Exception
    {
        Collection<Lease> leases = acquire(1, time, unit);
        return (leases != null) ? leases.iterator().next() : null;
    }

    /**
     * <p>Acquire <code>qty</code> leases. If there are not enough leases available, this method
     * blocks until either the maximum number of leases is increased enough or other clients/processes
     * close enough leases. However, this method will only block to a maximum of the time
     * parameters given. If time expires before all leases are acquired, the subset of acquired
     * leases are automatically closed.</p>
     * <p>The client must close the leases when it is done with them. You should do this in a
     * <code>finally</code> block. NOTE: You can use {@link #returnAll(Collection)} for this.</p>
     *
     * @param qty  number of leases to acquire
     * @param time time to wait
     * @param unit time unit
     * @return the new leases or null if time ran out
     * @throws Exception ZK errors, interruptions, etc.
     */
    // 具体的获取信号量锁的逻辑
    public Collection<Lease> acquire(int qty, long time, TimeUnit unit) throws Exception
    {
        long startMs = System.currentTimeMillis();
        boolean hasWait = (unit != null);
        // 计算等待时间
        long waitMs = hasWait ? TimeUnit.MILLISECONDS.convert(time, unit) : 0;

        // 每次获取信号量不能是0
        Preconditions.checkArgument(qty > 0, "qty cannot be 0");

        ImmutableList.Builder<Lease> builder = ImmutableList.builder();
        boolean success = false;
        try
        {
            // 做自减操作，每次就加一个锁，依次加
            while ( qty-- > 0 )
            {
                int retryCount = 0;
                long startMillis = System.currentTimeMillis();
                boolean isDone = false;
                while ( !isDone )
                {
                    // 执行加锁逻辑
                    switch ( internalAcquire1Lease(builder, startMs, hasWait, waitMs) )
                    {
                        case CONTINUE:
                        {
                            // 获取信号量成功，退出循环
                            isDone = true;
                            break;
                        }

                        case RETURN_NULL:
                        {
                            // 获取失败，返回null
                            return null;
                        }

                        case RETRY_DUE_TO_MISSING_NODE:
                        {
                            // gets thrown by internalAcquire1Lease when it can't find the lock node
                            // this can happen when the session expires, etc. So, if the retry allows, just try it all again
                            // 节点丢失，说明有bug啊，直接抛出异常
                            if ( !client.getZookeeperClient().getRetryPolicy().allowRetry(retryCount++, System.currentTimeMillis() - startMillis, RetryLoop.getDefaultRetrySleeper()) )
                            {
                                throw new KeeperException.NoNodeException("Sequential path not found - possible session loss");
                            }
                            // try again
                            break;
                        }
                    }
                }
            }
            success = true;
        }
        finally
        {
            if ( !success )
            {
                returnAll(builder.build());
            }
        }

        return builder.build();
    }

    private enum InternalAcquireResult
    {
        CONTINUE,
        RETURN_NULL,
        RETRY_DUE_TO_MISSING_NODE
    }

    static volatile CountDownLatch debugAcquireLatch = null;
    static volatile CountDownLatch debugFailedGetChildrenLatch = null;
    volatile CountDownLatch debugWaitLatch = null;

    // 核心的加锁逻辑，每次加1个
    private InternalAcquireResult internalAcquire1Lease(ImmutableList.Builder<Lease> builder, long startMs, boolean hasWait, long waitMs) throws Exception
    {
        if ( client.getState() != CuratorFrameworkState.STARTED )
        {
            return InternalAcquireResult.RETURN_NULL;
        }

        // 先获取lock的锁，这是互斥的锁，同一个时间只有一个客户端线程可以加锁成功的
        // 所以获取信号量锁也是先获取lock锁来排队
        if ( hasWait )
        {
            long thisWaitMs = getThisWaitMs(startMs, waitMs);
            if ( !lock.acquire(thisWaitMs, TimeUnit.MILLISECONDS) )
            {
                // 待超时时间的获取锁，失败直接返回RETURN_NULL状态
                return InternalAcquireResult.RETURN_NULL;
            }
        }
        else
        {
            // 获取失败直接wait卡主，等唤醒后继续
            lock.acquire();
        }

        Lease lease = null;
        boolean success = false;

        // 走到这里，说明当前线程获取到了lock的锁
        try
        {
            PathAndBytesable<String> createBuilder = client.create().creatingParentContainersIfNeeded().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL);
            // 创建存在获取信号量的客户端的标识的路径
            // 即：path=/semaphores/semaphore_01/leases/_c_a9302e20-de9c-4356-923a-274664d7676c-lease-0000000000
            // leasePath=/semaphores/semaphore_01/leases
            String path = (nodeData != null) ? createBuilder.forPath(ZKPaths.makePath(leasesPath, LEASE_BASE_NAME), nodeData) : createBuilder.forPath(ZKPaths.makePath(leasesPath, LEASE_BASE_NAME));
            // 获取客户端的标识,即nodeName=_c_a9302e20-de9c-4356-923a-274664d7676c-lease-0000000000
            String nodeName = ZKPaths.getNodeFromPath(path);
            // 新建lease
            lease = makeLease(path);

            if ( debugAcquireLatch != null )
            {
                debugAcquireLatch.await();
            }

            try
            {
                synchronized(this)
                {
                    // 自旋
                    for(;;)
                    {
                        List<String> children;
                        try
                        {
                            // 获取leasePath下的所有的节点
                            // 比如_c_a9302e20-de9c-4356-923a-274664d7676c-lease-0000000000，_c_a9302e20-de9c-4356-923a-274664d7676c-lease-0000000001，_c_a9302e20-de9c-4356-923a-274664d7676c-lease-0000000002等
                            children = client.getChildren().usingWatcher(watcher).forPath(leasesPath);
                        }
                        catch ( Exception e )
                        {
                            if ( debugFailedGetChildrenLatch != null )
                            {
                                debugFailedGetChildrenLatch.countDown();
                            }
                            throw e;
                        }
                        if ( !children.contains(nodeName) )
                        {
                            // 如果lease下面不包含当前的nodeName，说明当前节点就不在lease路径下，说明上面创建时就出了问题，这里抛出异常
                            log.error("Sequential path not found: " + path);
                            return InternalAcquireResult.RETRY_DUE_TO_MISSING_NODE;
                        }

                        if ( children.size() <= maxLeases )
                        {
                            // 当前lease下的节点数量还没达到maxLeases或者刚好达到maxLeases。
                            // 注意这里的children已经包含了当前线程创建的节点了。所以只要条件成立，就是获取信号量锁成功
                            // 这里break会退出当前循环，直接走到467行：success = true;这里
                            break;
                        }
                        // 走到这里，说明children.size() > maxLeases，即表明当前客户端不能获取信号量，因为已经被获取完了
                        // hasWait为true，等待指定时间
                        if ( hasWait )
                        {
                            // 获取此次需要等待的时间
                            long thisWaitMs = getThisWaitMs(startMs, waitMs);
                            if ( thisWaitMs <= 0 )
                            {
                                // 等待时间到了，直接返回RETURN_NULL
                                return InternalAcquireResult.RETURN_NULL;
                            }
                            if ( debugWaitLatch != null )
                            {
                                debugWaitLatch.countDown();
                            }
                            // 还需要等待，等待指定时间
                            // 下次有别的获取了信号量的释放了，会唤醒从这里继续往下执行
                            wait(thisWaitMs);
                        }
                        else
                        {
                            // 永久等待直到成功
                            if ( debugWaitLatch != null )
                            {
                                debugWaitLatch.countDown();
                            }
                            // 下次有别的获取了信号量的释放了，会唤醒从这里继续往下执行
                            wait();
                        }
                    }
                    success = true;
                }
            }
            finally
            {
                if ( !success )
                {
                    // 获取lease失败，退回lease
                    returnLease(lease);
                }

                client.removeWatchers();
            }
        }
        finally
        {
            // 释放锁，zk会通知所有监听者
            lock.release();
        }
        builder.add(Preconditions.checkNotNull(lease));
        // 获取信号量锁成功，这里返回CONTINUE
        return InternalAcquireResult.CONTINUE;
    }

    private long getThisWaitMs(long startMs, long waitMs)
    {
        long elapsedMs = System.currentTimeMillis() - startMs;
        return waitMs - elapsedMs;
    }

    // 创建一个Lease
    private Lease makeLease(final String path)
    {
        return new Lease()
        {
            @Override
            public void close() throws IOException
            {
                try
                {
                    client.delete().guaranteed().forPath(path);
                }
                catch ( KeeperException.NoNodeException e )
                {
                    log.warn("Lease already released", e);
                }
                catch ( Exception e )
                {
                    ThreadUtils.checkInterrupted(e);
                    throw new IOException(e);
                }
            }

            @Override
            public byte[] getData() throws Exception
            {
                return client.getData().forPath(path);
            }

            @Override
            public String getNodeName() {
                return ZKPaths.getNodeFromPath(path);
            }
        };
    }
}
