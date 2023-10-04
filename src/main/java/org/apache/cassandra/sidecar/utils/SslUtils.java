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

package org.apache.cassandra.sidecar.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Objects;

import io.vertx.core.net.JksOptions;
import io.vertx.core.net.KeyCertOptions;
import io.vertx.core.net.PfxOptions;
import io.vertx.core.net.SSLOptions;
import io.vertx.core.net.TrustOptions;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;

/**
 * Utility class for SSL related operations
 */
public class SslUtils
{
    /**
     * Given the parameters, validate the keystore can be loaded and is usable
     *
     * @param config the keystore configuration
     * @throws KeyStoreException        when there is an error accessing the keystore
     * @throws NoSuchAlgorithmException when the keystore type algorithm is not available
     * @throws IOException              when an IO exception occurs
     * @throws CertificateException     when a problem was encountered with the certificate
     */
    public static void validateSslOpts(KeyStoreConfiguration config) throws KeyStoreException,
                                                                            NoSuchAlgorithmException,
                                                                            IOException,
                                                                            CertificateException
    {
        Objects.requireNonNull(config, "config must be provided");

        KeyStore ks;

        if (config.type() != null)
            ks = KeyStore.getInstance(config.type());
        else if (config.path().endsWith("p12"))
            ks = KeyStore.getInstance("PKCS12");
        else if (config.path().endsWith("jks"))
            ks = KeyStore.getInstance("JKS");
        else
            throw new IllegalArgumentException("Unrecognized keystore format extension: "
                                               + config.path().substring(config.path().length() - 3));
        try (FileInputStream keystore = new FileInputStream(config.path()))
        {
            ks.load(keystore, config.password().toCharArray());
        }
    }

    public static void setKeyStoreConfiguration(SSLOptions options, KeyStoreConfiguration keystore)
    {
        KeyCertOptions keyCertOptions;
        switch (keystore.type())
        {
            case "JKS":
                keyCertOptions = new JksOptions().setPath(keystore.path()).setPassword(keystore.password());
                break;

            case "PKCS12":
                keyCertOptions = new PfxOptions().setPath(keystore.path()).setPassword(keystore.password());
                break;

            default:
                throw new UnsupportedOperationException("KeyStore with type " + keystore.type() + " is not supported");
        }
        options.setKeyCertOptions(keyCertOptions);
    }

    public static void setTrustStoreConfiguration(SSLOptions options, KeyStoreConfiguration truststore)
    {
        TrustOptions keyCertOptions;
        switch (truststore.type())
        {
            case "JKS":
                keyCertOptions = new JksOptions().setPath(truststore.path()).setPassword(truststore.password());
                break;

            case "PKCS12":
                keyCertOptions = new PfxOptions().setPath(truststore.path()).setPassword(truststore.password());
                break;

            default:
                throw new UnsupportedOperationException("TrustStore with type " + truststore.type()
                                                        + " is not supported");
        }
        options.setTrustOptions(keyCertOptions);
    }
}
