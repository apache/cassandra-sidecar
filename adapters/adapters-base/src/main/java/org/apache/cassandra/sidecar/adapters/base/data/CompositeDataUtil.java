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

package org.apache.cassandra.sidecar.adapters.base.data;

import java.util.NoSuchElementException;
import javax.management.openmbean.CompositeData;

/**
 * Utility class for operations with {@link CompositeData}
 */
public class CompositeDataUtil
{

    /**
     * Generic helper to extract attribute of a specific type from the CompositeData type.
     * @param data data being parsed
     * @param key attribute being extracted
     * @return attribute value
     * @param <T> return type
     */
    public static <T> T extractValue(CompositeData data, String key)
    {
        Object value = data.get(key);
        if (value == null)
        {
            throw new NoSuchElementException("No value is present for key: " + key);
        }
        try
        {
            return (T) value;
        }
        catch (ClassCastException cce)
        {
            throw new RuntimeException("Value type mismatched of key: " + key, cce);
        }
    }
}
