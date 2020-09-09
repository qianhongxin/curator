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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.curator.RetryLoop;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

// 加锁释放锁的具体操作zk的逻辑
public class LockInternals
{
    private final WatcherRemoveCuratorFramework     client;
    // 锁路径
    // 举例：/locks/lock_01/_c_0abad917-53a6-4ed9-ac96-bfac3327be0d-lock-0000000001
    private final String                            path;
    // 基本路径：/locks/lock_01
    private final String                            basePath;
    private final LockInternalsDriver               driver;
    // 锁的名字，可以传，不传就是默认的lock-
    private final String                            lockName;
    private final AtomicReference<RevocationSpec>   revocable = new AtomicReference<RevocationSpec>(null);
    // 监听器，当获取锁失败时加一个监听
    private final CuratorWatcher                    revocableWatcher = new CuratorWatcher()
    {
        @Override
        public void process(WatchedEvent event) throws Exception
        {
            if ( event.getType() == Watcher.Event.EventType.NodeDataChanged )
            {
                checkRevocableWatcher(event.getPath());
            }
        }
    };

    // 监听器
    private final Watcher watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            // LockInternals.this就是当前的lockInternals对象
            client.postSafeNotify(LockInternals.this);
        }
    };

    // 最大允许获取锁的线程数量
    private volatile int    maxLeases;

    static final byte[]             REVOKE_MESSAGE = "__REVOKE__".getBytes();

    /**
     * Attempt to delete the lock node so that sequence numbers get reset
     *
     * @throws Exception errors
     */
    public void clean() throws Exception
    {
        try
        {
            client.delete().forPath(basePath);
        }
        catch ( KeeperException.BadVersionException ignore )
        {
            // ignore - another thread/process got the lock
        }
        catch ( KeeperException.NotEmptyException ignore )
        {
            // ignore - other threads/processes are waiting
        }
    }

    LockInternals(CuratorFramework client, LockInternalsDriver driver, String path, String lockName, int maxLeases)
    {
        this.driver = driver;
        this.lockName = lockName;
        this.maxLeases = maxLeases;

        this.client = client.newWatcherRemoveCuratorFramework();
        this.basePath = PathUtils.validatePath(path);
        this.path = ZKPaths.makePath(path, lockName);
    }

    // 重置最大允许获取锁的线程数量，然后执行唤醒操作，唤醒其他等待加锁的节点
    synchronized void setMaxLeases(int maxLeases)
    {
        this.maxLeases = maxLeases;
        notifyAll();
    }

    void makeRevocable(RevocationSpec entry)
    {
        revocable.set(entry);
    }

    final void releaseLock(String lockPath) throws Exception
    {
        // 移除掉当前LockInternals关联的监听器Workeres
        client.removeWatchers();
        revocable.set(null);
        // 删除锁节点路径
        deleteOurPath(lockPath);
    }

    CuratorFramework getClient()
    {
        return client;
    }

    public static Collection<String> getParticipantNodes(CuratorFramework client, final String basePath, String lockName, LockInternalsSorter sorter) throws Exception
    {
        List<String>        names = getSortedChildren(client, basePath, lockName, sorter);
        Iterable<String>    transformed = Iterables.transform
            (
                names,
                new Function<String, String>()
                {
                    @Override
                    public String apply(String name)
                    {
                        return ZKPaths.makePath(basePath, name);
                    }
                }
            );
        return ImmutableList.copyOf(transformed);
    }

    // 对basePath下的节点做排序
    public static List<String> getSortedChildren(CuratorFramework client, String basePath, final String lockName, final LockInternalsSorter sorter) throws Exception
    {
        try
        {
            // 所有basePath（/locks/lock_01）下的所有加锁客户端对应的标识
            // 即_c_0abad917-53a6-4ed9-ac96-bfac3327be0d-lock-0000000001，_c_0abad917-53a6-4ed9-ac96-bfac3327be0d-lock-0000000002等
            List<String> children = client.getChildren().forPath(basePath);
            List<String> sortedList = Lists.newArrayList(children);
            Collections.sort
            (
                sortedList,
                new Comparator<String>()
                {
                    @Override
                    public int compare(String lhs, String rhs)
                    {
                        return sorter.fixForSorting(lhs, lockName).compareTo(sorter.fixForSorting(rhs, lockName));
                    }
                }
            );
            return sortedList;
        }
        catch ( KeeperException.NoNodeException ignore )
        {
            return Collections.emptyList();
        }
    }

    public static List<String> getSortedChildren(final String lockName, final LockInternalsSorter sorter, List<String> children)
    {
        List<String> sortedList = Lists.newArrayList(children);
        Collections.sort
        (
            sortedList,
            new Comparator<String>()
            {
                @Override
                public int compare(String lhs, String rhs)
                {
                    return sorter.fixForSorting(lhs, lockName).compareTo(sorter.fixForSorting(rhs, lockName));
                }
            }
        );
        return sortedList;
    }

    List<String> getSortedChildren() throws Exception
    {
        return getSortedChildren(client, basePath, lockName, driver);
    }

    String getLockName()
    {
        return lockName;
    }

    LockInternalsDriver getDriver()
    {
        return driver;
    }

    // 加锁处理逻辑
    String attemptLock(long time, TimeUnit unit, byte[] lockNodeBytes) throws Exception
    {
        final long      startMillis = System.currentTimeMillis();
        final Long      millisToWait = (unit != null) ? unit.toMillis(time) : null;
        final byte[]    localLockNodeBytes = (revocable.get() != null) ? new byte[0] : lockNodeBytes;
        int             retryCount = 0;

        String          ourPath = null;
        boolean         hasTheLock = false;
        boolean         isDone = false;
        while ( !isDone )
        {
            isDone = true;

            try
            {
                // 创建临时顺序节点，任意客户端只要没加过锁即客户端路径在zk中不存在，都会去创建
                ourPath = driver.createsTheLock(client, path, localLockNodeBytes);
                // 执行获取锁操作
                hasTheLock = internalLockLoop(startMillis, millisToWait, ourPath);
            }
            catch ( KeeperException.NoNodeException e )
            {
                // gets thrown by StandardLockInternalsDriver when it can't find the lock node
                // this can happen when the session expires, etc. So, if the retry allows, just try it all again
                if ( client.getZookeeperClient().getRetryPolicy().allowRetry(retryCount++, System.currentTimeMillis() - startMillis, RetryLoop.getDefaultRetrySleeper()) )
                {
                    isDone = false;
                }
                else
                {
                    throw e;
                }
            }
        }

        if ( hasTheLock )
        {
            return ourPath;
        }

        return null;
    }

    private void checkRevocableWatcher(String path) throws Exception
    {
        RevocationSpec  entry = revocable.get();
        if ( entry != null )
        {
            try
            {
                byte[]      bytes = client.getData().usingWatcher(revocableWatcher).forPath(path);
                if ( Arrays.equals(bytes, REVOKE_MESSAGE) )
                {
                    entry.getExecutor().execute(entry.getRunnable());
                }
            }
            catch ( KeeperException.NoNodeException ignore )
            {
                // ignore
            }
        }
    }

    private boolean internalLockLoop(long startMillis, Long millisToWait, String ourPath) throws Exception
    {
        boolean     haveTheLock = false;
        boolean     doDelete = false;
        try
        {
            if ( revocable.get() != null )
            {
                client.getData().usingWatcher(revocableWatcher).forPath(ourPath);
            }

            // 自旋操作，获取锁
            while ( (client.getState() == CuratorFrameworkState.STARTED) && !haveTheLock )
            {
                // 对basePath下所有的客户端创建的临时节点从小到大排序
                // 只要客户端去获取锁，如果没有加过锁，就会在zk上创建一个临时顺序节点
                // 拿到的是排好序的锁节点，比如[_c_0abad917-53a6-4ed9-ac96-bfac3327be0d-lock-0000000001,_c_0abad917-53a6-4ed9-ac96-bfac3327be0d-lock-0000000002]
                List<String>        children = getSortedChildren();
                // 截取basePath之后的字符串
                // 即/locks/lock_01/_c_0abad917-53a6-4ed9-ac96-bfac3327be0d-lock-0000000001 =》_c_0abad917-53a6-4ed9-ac96-bfac3327be0d-lock-0000000001
                // 最终得到的sequenceNodeName为_c_0abad917-53a6-4ed9-ac96-bfac3327be0d-lock-0000000001
                String              sequenceNodeName = ourPath.substring(basePath.length() + 1); // +1 to include the slash
                // 执行获取锁逻辑，返回一个predicateResults
                PredicateResults    predicateResults = driver.getsTheLock(client, children, sequenceNodeName, maxLeases);
                // 为ture说明当前客户端当前线程加锁成功
                if ( predicateResults.getsTheLock() )
                {
                    haveTheLock = true;
                }
                else
                {
                    // 走到这，说明加锁失败

                    // 获取当前客户端当前线程需要监听的节点路径名称
                    String  previousSequencePath = basePath + "/" + predicateResults.getPathToWatch();

                    synchronized(this)
                    {
                        try
                        {
                            // use getData() instead of exists() to avoid leaving unneeded watchers which is a type of resource leak
                            // 监听前置节点
                            client.getData().usingWatcher(watcher).forPath(previousSequencePath);
                            if ( millisToWait != null )
                            {
                                // 走到这，说明加锁时传入了超时时间
                                // 计算等待时间
                                millisToWait -= (System.currentTimeMillis() - startMillis);
                                startMillis = System.currentTimeMillis();
                                if ( millisToWait <= 0 )
                                {
                                    // 走到这，说明加锁失败的等待时间已到，这里标识需要删除之前加锁节点
                                    doDelete = true;    // timed out - delete our node
                                    break;
                                }
                                // 等待时间还没到，等待指定时间
                                // 等下次别的客户端释放锁，然后注册的监听器会收到回调，然后会触发notifyAll。然后当前客户端当前线程会从这里醒来继续往下执行
                                wait(millisToWait);
                            }
                            else
                            {
                                // 如果加锁时没有传入超时时间，这里就一直等，直到加锁成功。
                                // 走到这里，需要等下次别的客户端释放锁，然后注册的监听器会收到回调，然后会触发notifyAll。然后当前客户端当前线程会从这里醒来继续往下执行
                                wait();
                            }
                        }
                        catch ( KeeperException.NoNodeException e )
                        {
                            // it has been deleted (i.e. lock released). Try to acquire again
                        }
                    }
                }
            }
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            // 抛出异常，也直接删除在zk中的设置的节点
            doDelete = true;
            throw e;
        }
        finally
        {
            // 走到这，说明已经等待超时了，直接删除在zk中的设置的节点
            if ( doDelete )
            {
                deleteOurPath(ourPath);
            }
        }
        return haveTheLock;
    }

    private void deleteOurPath(String ourPath) throws Exception
    {
        try
        {
            client.delete().guaranteed().forPath(ourPath);
        }
        catch ( KeeperException.NoNodeException e )
        {
            // ignore - already deleted (possibly expired session, etc.)
        }
    }
}
