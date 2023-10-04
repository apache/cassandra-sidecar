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
import java.util.function.Function;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.X509KeyManager;

import io.vertx.core.Vertx;
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

    public static void setKeyStoreConfiguration(SSLOptions options, KeyStoreConfiguration keystore, long timestamp)
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
        options.setKeyCertOptions(new WrappedKeyCertOptions(timestamp, keyCertOptions));
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

    /**
     * Vertx makes a determination on whether the SSL context should be reloaded based on whether the
     * {@link SSLOptions} have changed. This means that if the keystore certificate file changed but the file
     * name remains the same, the SSL context is not considered to have been changed.
     *
     * <p>This class allows for us to keep track of the last modified timestamp of the underlying file, and if
     * the underlying file changes, we propagate that information to the {@link SSLOptions} via the equality method in
     * this class. When the old timestamp and new timestamp differ, we'll force the SSL context reloading in vertx.
     */
    static class WrappedKeyCertOptions implements KeyCertOptions
    {
        private final long timestamp;
        private final KeyCertOptions delegate;

        WrappedKeyCertOptions(long timestamp, KeyCertOptions delegate)
        {
            this.timestamp = timestamp;
            this.delegate = delegate;
        }

        @Override
        public KeyCertOptions copy()
        {
            return new WrappedKeyCertOptions(timestamp, delegate.copy());
        }

        @Override
        public KeyManagerFactory getKeyManagerFactory(Vertx vertx) throws Exception
        {
            return delegate.getKeyManagerFactory(vertx);
        }

        @Override
        public Function<String, X509KeyManager> keyManagerMapper(Vertx vertx) throws Exception
        {
            return delegate.keyManagerMapper(vertx);
        }

        @Override
        public Function<String, KeyManagerFactory> keyManagerFactoryMapper(Vertx vertx) throws Exception
        {
            return delegate.keyManagerFactoryMapper(vertx);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WrappedKeyCertOptions that = (WrappedKeyCertOptions) o;
            return timestamp == that.timestamp
                   && Objects.equals(delegate, that.delegate);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(timestamp, delegate);
        }
    }
}
