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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.curator.framework.CuratorFramework;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * <p>
 *    A re-entrant read/write mutex that works across JVMs. Uses Zookeeper to hold the lock. All processes
 *    in all JVMs that use the same lock path will achieve an inter-process critical section. Further, this mutex is
 *    "fair" - each user will get the mutex in the order requested (from ZK's point of view).
 * </p>
 *
 * <p>
 *    A read write lock maintains a pair of associated locks, one for read-only operations and one
 *    for writing. The read lock may be held simultaneously by multiple reader processes, so long as
 *    there are no writers. The write lock is exclusive.
 * </p>
 *
 * <p>
 *    <b>Reentrancy</b><br>
 *    This lock allows both readers and writers to reacquire read or write locks in the style of a
 *    re-entrant lock. Non-re-entrant readers are not allowed until all write locks held by the
 *    writing thread/process have been released. Additionally, a writer can acquire the read lock, but not
 *    vice-versa. If a reader tries to acquire the write lock it will never succeed.<br><br>
 *
 *    <b>Lock downgrading</b><br>
 *    Re-entrancy also allows downgrading from the write lock to a read lock, by acquiring the write
 *    lock, then the read lock and then releasing the write lock. However, upgrading from a read
 *    lock to the write lock is not possible.
 * </p>
 */
// 可重入读写锁，基于继承自InterProcessMutex的InternalInterProcessMutex实现
public class InterProcessReadWriteLock
{
    // 读锁
    private final InterProcessMutex readMutex;
    // 写锁
    private final InterProcessMutex writeMutex;

    // must be the same length. LockInternals depends on it
    // 读锁名称前缀
    private static final String READ_LOCK_NAME  = "__READ__";
    // 写锁名称前缀
    private static final String WRITE_LOCK_NAME = "__WRIT__";

    // 锁排序器
    private static class SortingLockInternalsDriver extends StandardLockInternalsDriver
    {
        @Override
        public final String fixForSorting(String str, String lockName)
        {
            str = super.fixForSorting(str, READ_LOCK_NAME);
            str = super.fixForSorting(str, WRITE_LOCK_NAME);
            return str;
        }
    }

    // 实现了InterProcessMutex
    private static class InternalInterProcessMutex extends InterProcessMutex
    {
        // 锁前缀
        private final String lockName;
        // lockData
        private final byte[] lockData;

        InternalInterProcessMutex(CuratorFramework client, String path, String lockName, byte[] lockData, int maxLeases, LockInternalsDriver driver)
        {
            super(client, path, lockName, maxLeases, driver);
            this.lockName = lockName;
            this.lockData = lockData;
        }

        @Override
        public Collection<String> getParticipantNodes() throws Exception
        {
            Collection<String>  nodes = super.getParticipantNodes();
            Iterable<String>    filtered = Iterables.filter
            (
                nodes,
                new Predicate<String>()
                {
                    @Override
                    public boolean apply(String node)
                    {
                        return node.contains(lockName);
                    }
                }
            );
            return ImmutableList.copyOf(filtered);
        }

        @Override
        protected byte[] getLockNodeBytes()
        {
            return lockData;
        }
    }

  /**
    * @param client the client
    * @param basePath path to use for locking
    */
    public InterProcessReadWriteLock(CuratorFramework client, String basePath)
    {
        this(client, basePath, null);
    }

  /**
    * @param client the client
    * @param basePath path to use for locking
    * @param lockData the data to store in the lock nodes
    */
    public InterProcessReadWriteLock(CuratorFramework client, String basePath, byte[] lockData)
    {
        lockData = (lockData == null) ? null : Arrays.copyOf(lockData, lockData.length);

        // 创建写锁，基于InternalInterProcessMutex
        writeMutex = new InternalInterProcessMutex
        (
            client,
            // 锁的所属路径
            basePath,
            // 锁前缀
            WRITE_LOCK_NAME,
            // 锁数据
            lockData,
            1,
            // 锁排序器
            new SortingLockInternalsDriver()
            {
                @Override
                public PredicateResults getsTheLock(CuratorFramework client, List<String> children, String sequenceNodeName, int maxLeases) throws Exception
                {
                    return super.getsTheLock(client, children, sequenceNodeName, maxLeases);
                }
            }
        );

        // 创建读锁，基于InternalInterProcessMutex
        readMutex = new InternalInterProcessMutex
        (
            client,
            basePath,
                // 锁前缀
            READ_LOCK_NAME,
            lockData,
            Integer.MAX_VALUE,
            new SortingLockInternalsDriver()
            {
                @Override
                public PredicateResults getsTheLock(CuratorFramework client, List<String> children, String sequenceNodeName, int maxLeases) throws Exception
                {
                    return readLockPredicate(children, sequenceNodeName);
                }
            }
        );
    }

    /**
     * Returns the lock used for reading.
     *
     * @return read lock
     */
    public InterProcessMutex     readLock()
    {
        return readMutex;
    }

    /**
     * Returns the lock used for writing.
     *
     * @return write lock
     */
    public InterProcessMutex     writeLock()
    {
        return writeMutex;
    }

    // 供读锁调用，判断读锁是否可以加
    private PredicateResults readLockPredicate(List<String> children, String sequenceNodeName) throws Exception
    {
        // 判断当前线程是否加了写锁
        if ( writeMutex.isOwnedByCurrentThread() )
        {
            // 标识当前线程已经加了写锁了，这里加读锁也是可以的，加读锁成功
            // redisson也是，同一个客户端同一个线程，先加读锁后还可以加写锁
            return new PredicateResults(null, true);
        }

        int         index = 0;
        int         firstWriteIndex = Integer.MAX_VALUE;
        int         ourIndex = -1;
        // 遍历basePath（/locks/lock_01）下的所有的加锁客户端的写入标识
        // 即_c_0abad917-53a6-4ed9-ac96-bfac3327be0d__WRIT__0000000001，_c_0abad917-53a6-4ed9-ac96-bfac3327be0d__READ__0000000002，_c_0abad917-53a6-4ed9-ac96-bfac3327be0d__WRIT__0000000003等
        for ( String node : children )
        {
            // 判断node是否包含写锁字符串标识
            if ( node.contains(WRITE_LOCK_NAME) )
            {
                // 将第一个写锁索引记录到firstWriteIndex
                firstWriteIndex = Math.min(index, firstWriteIndex);
            }
            else if ( node.startsWith(sequenceNodeName) )
            {
                // 将index赋值给ourIndex
                ourIndex = index;
                break;
            }

            ++index;
        }

        // ourIndex < 0 基础校验
        StandardLockInternalsDriver.validateOurIndex(sequenceNodeName, ourIndex);
        // 如果当前加锁节点sequenceNodeName在children的索引值小于children中第一个写锁节点的索引值。则说明此次加读锁是成功的
        // 说明读写锁也是基于公平机制的
        boolean     getsTheLock = (ourIndex < firstWriteIndex);
        // getsTheLock为true说明加读锁成功，不用监听其他节点释放锁。
        // getsTheLock为false说明加读锁失败，获取了children中第一个写锁节点，后面对这个节点加监听
            // 但是这个给第一个写锁加监听有一个问题：
                //问题就是容易引发羊群效应，比如第一个写锁后面排了2000个读锁节点，那当这个写锁释放后，需要通知2000个加锁节点来争抢锁，这个网络通信的带宽占用挺大啊。如果当前zk中有多个这种情况的写锁，那网络带宽严重占用啊。这是缺点。
                // 改进羊群效应：改进225行的算法，不要只监听第一个写锁，而是监听当前读锁节点前面最近的一个写锁节点。这样比如2000个读锁节点就分可以分摊到各个写锁节点的上，减轻羊群效应。如果就一个写锁节点那就无解了
                // InterProcessMutex，信号量，不可重入锁，读写锁的写锁那些都没有羊群效应问题，他们都是基于InterProcessMutex实现的加锁，等待节点都是监听前一个节点，公平的。既然都是监听前一个节点，就不存在羊群效应啊。只有这个读写锁的读 锁这里实现有问题，可以优化。
        String      pathToWatch = getsTheLock ? null : children.get(firstWriteIndex);
        return new PredicateResults(pathToWatch, getsTheLock);
    }
}
