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

package org.apache.cassandra.sidecar.testing;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.pool.TypePool;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

/**
 * Utils to install the ByteBuddy method interceptor for bootstrap
 */
public class BootstrapBBUtils
{
    /**
     * Note that the test class _must_ define the `setBootstrapState` method in order for the installed intercepter to be effective.
     * See {@code ReplacementTest.BBHelperReplacementsNode} for example
     */
    public static void installSetBoostrapStateIntercepter(ClassLoader cl, Class<?> interceptor)
    {
        TypePool typePool = TypePool.Default.of(cl);
        TypeDescription description = typePool.describe("org.apache.cassandra.db.SystemKeyspace")
                                              .resolve();
        new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                       .method(named("setBootstrapState").and(takesArguments(1)))
                       .intercept(MethodDelegation.to(interceptor))
                       // Defer class loading until all dependencies are loaded
                       .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                       .load(cl, ClassLoadingStrategy.Default.INJECTION);
    }
}
