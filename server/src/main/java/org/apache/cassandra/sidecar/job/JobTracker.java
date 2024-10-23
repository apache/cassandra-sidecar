/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.job;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jetbrains.annotations.NotNull;

/**
 * Tracks and stores the results of long-running jobs running on the sidecar
 */
public class JobTracker extends LinkedHashMap<UUID, Job>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(JobTracker.class);
    int capacity;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public JobTracker(final int initialCapacity)
    {
        super(initialCapacity);
        this.capacity = initialCapacity;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Job put(final UUID key, final Job value)
    {
        lock.writeLock().lock();
        try
        {
            return super.put(key, value);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Job putIfAbsent(final UUID key, final Job value)
    {
        lock.writeLock().lock();
        try
        {
            return super.putIfAbsent(key, value);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAll(final Map<? extends UUID, ? extends Job> m)
    {
        lock.writeLock().lock();
        try
        {
            super.putAll(m);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Job remove(final Object key)
    {
        lock.writeLock().lock();
        try
        {
            return super.remove(key);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear()
    {
        lock.writeLock().lock();
        try
        {
            super.clear();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Job get(final Object key)
    {
        lock.readLock().lock();
        try
        {
            return super.get(key);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(final Object key)
    {
        lock.readLock().lock();
        try
        {
            return super.containsKey(key);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size()
    {
        lock.readLock().lock();
        try
        {
            return super.size();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty()
    {
        lock.readLock().lock();
        try
        {
            return super.isEmpty();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object clone()
    {
        lock.readLock().lock();
        try
        {
            return super.clone();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean removeEldestEntry(final Map.Entry<UUID, Job> eldest)
    {
        if (size() <= capacity)
        {
            return false;
        }

        LOGGER.warn("Job tracker reached max size, so expiring job uuid={}", eldest.getKey());
        return true;
    }


    /**
     * Returns an immutable copy of the underlying map, to provide a consistent view of the map, minimizing contention
     *
     * @return an immutable copy of the underlying mapping
     */
    @NotNull
    ImmutableMap<UUID, Job> getJobsView()
    {
        lock.readLock().lock();
        try
        {
            return ImmutableMap.copyOf(this);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
}
