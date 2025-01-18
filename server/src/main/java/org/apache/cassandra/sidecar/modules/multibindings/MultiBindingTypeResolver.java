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

import java.util.Map;

import org.jetbrains.annotations.NotNull;

/**
 * Resolves the types contained in map bindings created from {@link com.google.inject.multibindings.MapBinder}
 * Note that the key type of the map is {@code Class<? extends ClassKey>} in order to permit flexible extension
 * @param <V> map binding value type
 */
public interface MultiBindingTypeResolver<V>
{
    /**
     * Resolves the types contained in map bindings created from {@link com.google.inject.multibindings.MapBinder}
     *
     * @return resolved map bindings. The returned value should never be null.
     */
    @NotNull
    Map<Class<? extends ClassKey>, V> resolve();
}
