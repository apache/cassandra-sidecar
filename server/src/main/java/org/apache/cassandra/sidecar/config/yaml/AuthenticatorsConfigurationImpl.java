///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.cassandra.sidecar.config.yaml;
//
//import com.fasterxml.jackson.annotation.JsonProperty;
//import org.apache.cassandra.sidecar.config.AuthenticatorsConfiguration;
//import org.apache.cassandra.sidecar.config.MutualTlsAuthenticatorConfiguration;
//
///**
// * {@inheritDoc}
// */
//public class AuthenticatorsConfigurationImpl implements AuthenticatorsConfiguration
//{
//    private static final MutualTlsAuthenticatorConfiguration DEFAULT_MTLS_AUTHENTICATOR_CONFIGURATION = null;
//
//    @JsonProperty(value = "mtls_authenticator")
//    protected final MutualTlsAuthenticatorConfiguration mTlsAuthenticatorConfiguration;
//
//    public AuthenticatorsConfigurationImpl()
//    {
//        this(DEFAULT_MTLS_AUTHENTICATOR_CONFIGURATION);
//    }
//
//    public AuthenticatorsConfigurationImpl(MutualTlsAuthenticatorConfiguration mTlsAuthenticatorConfiguration)
//    {
//        this.mTlsAuthenticatorConfiguration = mTlsAuthenticatorConfiguration;
//    }
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    @JsonProperty(value = "mtls_authenticator")
//    public MutualTlsAuthenticatorConfiguration mTlsAuthenticatorConfiguration()
//    {
//        return mTlsAuthenticatorConfiguration;
//    }
//}
