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

package io.vertx.ext.auth.mtls;

import java.util.function.Function;

import io.vertx.ext.auth.User;

/**
 * {@inheritDoc}
 */
public class MutualTlsIdentityValidatorImpl implements MutualTlsIdentityValidator
{
    private final Function<String, Boolean> validIdentity;

    public MutualTlsIdentityValidatorImpl(Function<String, Boolean> validIdentity)
    {
        this.validIdentity = validIdentity;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isValidIdentity(String identity)
    {
        return validIdentity.apply(identity);
    }

    /**
     * {@inheritDoc}
     */
    public User userFromIdentity(String identity)
    {
        return User.fromName(identity);
    }
}
