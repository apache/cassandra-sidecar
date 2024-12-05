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
import java.util.concurrent.TimeUnit;

import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.sidecar.testing.BootstrapBBUtils;

import static org.apache.cassandra.sidecar.testing.IntegrationTestBase.awaitLatchOrTimeout;

/**
 * ByteBuddy Helper for a single node joining
 * Note that the helper cannot be used by multiple tests simultaneously in the same JVM
 */
public class BBHelperJoiningNode
{
    public static CountDownLatch transientStateStart = new CountDownLatch(1);
    public static CountDownLatch transientStateEnd = new CountDownLatch(1);

    public static void install(ClassLoader cl, int nodeNumber, int joiningNodeIndex)
    {
        if (nodeNumber == joiningNodeIndex)
        {
            BootstrapBBUtils.installSetBoostrapStateInterceptor(cl, BBHelperJoiningNode.class);
        }
    }

    public static void setBootstrapState(SystemKeyspace.BootstrapState state, @SuperCall Callable<Void> orig) throws Exception
    {
        if (state == SystemKeyspace.BootstrapState.COMPLETED)
        {
            // trigger bootstrap start and wait until bootstrap is ready from test
            transientStateStart.countDown();
            awaitLatchOrTimeout(transientStateEnd, 2, TimeUnit.MINUTES, "transientStateEnd");
        }
        orig.call();
    }

    public static void reset()
    {
        transientStateStart = new CountDownLatch(1);
        transientStateEnd = new CountDownLatch(1);
    }
}
