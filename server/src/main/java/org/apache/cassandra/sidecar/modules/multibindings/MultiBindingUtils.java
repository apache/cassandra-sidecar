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

package org.apache.cassandra.sidecar.modules.multibindings;

import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;

/**
 * Utility methods for multibinding
 */
public class MultiBindingUtils
{
    private MultiBindingUtils()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a {@link MapBinder} that uses {@link ClassKey} class as key with the supplied class for value.
     * <p>Contributing mapbindings from different modules is supported. For example, it is okay to have
     * both {@code CandyModule} and {@code ChipsModule} both create their own {@code MapBinder<String,
     * Snack>}, and to each contribute bindings to the snacks map. When that map is injected, it will
     * contain entries from both modules.
     *
     * @param binder binder
     * @param valueClass class of the value in the map binder
     * @return map binder
     * @param <V> type of the value
     */
    public static <V> MapBinder<Class<? extends ClassKey>, V> newClassKeyClassMapBinder(Binder binder, Class<V> valueClass)
    {
        TypeLiteral<Class<? extends ClassKey>> keyType = new TypeLiteral<>() {};
        TypeLiteral<V> valueType = TypeLiteral.get(valueClass);
        return MapBinder.newMapBinder(binder, keyType, valueType);
    }
}
