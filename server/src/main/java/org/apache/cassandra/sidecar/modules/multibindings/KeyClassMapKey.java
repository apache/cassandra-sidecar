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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.google.inject.multibindings.MapKey;
import com.google.inject.multibindings.ProvidesIntoMap;

/**
 * Allows {@literal @}{@link ProvidesIntoMap} to specify a {@link ClassKey} class as map key.
 * The class must extends from {@link ClassKey}
 * <p>The annotation name {@link KeyClassMapKey} is read as "map key type is of {@link ClassKey} class"
 */
@MapKey(unwrapValue = true)
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface KeyClassMapKey
{
    Class<? extends ClassKey> value();
}
