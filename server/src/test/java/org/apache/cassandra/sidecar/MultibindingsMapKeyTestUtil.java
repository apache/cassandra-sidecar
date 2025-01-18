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

package org.apache.cassandra.sidecar;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.apache.cassandra.sidecar.modules.multibindings.ClassKey;
import org.apache.cassandra.sidecar.modules.multibindings.MultiBindingTypeResolver;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;

/**
 * Test utility to find the bound instance from mapBinder
 */
public class MultibindingsMapKeyTestUtil
{
    private MultibindingsMapKeyTestUtil()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Find the periodic task of the key from the map binder
     * @param injector injector
     * @param key key
     * @return periodic task
     */
    public static <T extends PeriodicTask> T findPeriodicTask(Injector injector, Class<? extends ClassKey> key)
    {
        TypeLiteral<MultiBindingTypeResolver<PeriodicTask>> type = new TypeLiteral<>(){};
        //noinspection unchecked
        return (T) find(injector, type, key);
    }

    private static <T> T find(Injector injector, TypeLiteral<MultiBindingTypeResolver<T>> type, Class<? extends ClassKey> key)
    {
        return injector.getInstance(Key.get(type)).resolve().get(key);
    }
}
