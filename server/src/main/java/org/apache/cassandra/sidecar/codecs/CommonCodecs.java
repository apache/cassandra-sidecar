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

package org.apache.cassandra.sidecar.codecs;

import io.vertx.core.eventbus.impl.codecs.BooleanMessageCodec;
import io.vertx.core.eventbus.impl.codecs.ByteArrayMessageCodec;
import io.vertx.core.eventbus.impl.codecs.IntMessageCodec;
import io.vertx.core.eventbus.impl.codecs.ShortMessageCodec;
import io.vertx.core.eventbus.impl.codecs.StringMessageCodec;

/**
 * Codecs common to Sidecar
 */
public class CommonCodecs
{
    public static final StringMessageCodec STRING = new StringMessageCodec();
    public static final ShortMessageCodec SHORT = new ShortMessageCodec();
    public static final ByteArrayMessageCodec BYTE_ARRAY = new ByteArrayMessageCodec();
    public static final IntMessageCodec INT = new IntMessageCodec();
    public static final BooleanMessageCodec BOOL = new BooleanMessageCodec();
}
