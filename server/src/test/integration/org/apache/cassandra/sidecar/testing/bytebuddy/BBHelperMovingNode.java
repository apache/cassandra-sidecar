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

package org.apache.cassandra.sidecar.testing.bytebuddy;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.sidecar.testing.IntegrationTestBase.awaitLatchOrTimeout;

/**
 * ByteBuddy Helper for a single moving node
 * Note that the helper cannot be used by multiple tests simultaneously in the same JVM
 */
public class BBHelperMovingNode
{
    public static CountDownLatch transientStateStart = new CountDownLatch(1);
    public static CountDownLatch transientStateEnd = new CountDownLatch(1);

    public static void install(ClassLoader cl, int nodeNumber, int movingNodeIndex)
    {
        if (nodeNumber == movingNodeIndex)
        {
            TypePool typePool = TypePool.Default.of(cl);
            TypeDescription description = typePool.describe("org.apache.cassandra.service.RangeRelocator")
                                                  .resolve();
            new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                           .method(named("stream"))
                           .intercept(MethodDelegation.to(BBHelperMovingNode.class))
                           // Defer class loading until all dependencies are loaded
                           .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }
    }

    @SuppressWarnings("unused")
    public static Future<?> stream(@SuperCall Callable<Future<?>> orig) throws Exception
    {
        Future<?> res = orig.call();
        transientStateStart.countDown();
        awaitLatchOrTimeout(transientStateEnd, 2, TimeUnit.MINUTES, "transientStateEnd");
        return res;
    }

    public static void reset()
    {
        transientStateStart = new CountDownLatch(1);
        transientStateEnd = new CountDownLatch(1);
    }
}
