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

import java.io.PrintStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.http.HttpServer;
import org.apache.cassandra.sidecar.cdc.CDCReaderService;
import org.apache.cassandra.sidecar.utils.SslUtils;
/**
 * Main class for initiating the Cassandra sidecar
 */
@Singleton
public class CassandraSidecarDaemon
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraSidecarDaemon.class);
    private final HttpServer server;
    private final Configuration config;
    private final CDCReaderService cdcReaderService;

    @Inject
    public CassandraSidecarDaemon(HttpServer server, Configuration config, CDCReaderService cdcReaderService)
    {
        this.server = server;
        this.config = config;
        this.cdcReaderService = cdcReaderService;
    }

    public void start()
    {
        banner(System.out);
        validate();
        logger.info("Starting Cassandra Sidecar on {}:{}", config.getHost(), config.getPort());
        cdcReaderService.start();
        server.listen(config.getPort(), config.getHost());
    }

    public void stop()
    {
        logger.info("Stopping Cassandra Sidecar");
        server.close();
    }

    private void banner(PrintStream out)
    {
        out.println(" _____                               _              _____ _     _                     \n" +
                    "/  __ \\                             | |            /  ___(_)   | |                    \n" +
                    "| /  \\/ __ _ ___ ___  __ _ _ __   __| |_ __ __ _   \\ `--. _  __| | ___  ___ __ _ _ __ \n" +
                    "| |    / _` / __/ __|/ _` | '_ \\ / _` | '__/ _` |   `--. \\ |/ _` |/ _ \\/ __/ _` | '__|\n" +
                    "| \\__/\\ (_| \\__ \\__ \\ (_| | | | | (_| | | | (_| |  /\\__/ / | (_| |  __/ (_| (_| | |   \n" +
                    " \\____/\\__,_|___/___/\\__,_|_| |_|\\__,_|_|  \\__,_|  \\____/|_|\\__,_|\\___|\\___\\__,_|_|\n" +
                    "                                                                                      \n" +
                    "                                                                                      ");
    }

    private void validate()
    {
        if (config.isSslEnabled())
        {
            try
            {
                if (config.getKeyStorePath() == null || config.getKeystorePassword() == null)
                    throw new IllegalArgumentException("keyStorePath and keyStorePassword must be set if ssl enabled");

                SslUtils.validateSslOpts(config.getKeyStorePath(), config.getKeystorePassword());

                if (config.getTrustStorePath() != null && config.getTruststorePassword() != null)
                    SslUtils.validateSslOpts(config.getTrustStorePath(), config.getTruststorePassword());
            }
            catch (Exception e)
            {
                throw new RuntimeException("Invalid keystore parameters for SSL", e);
            }
        }

    }


    public static void main(String[] args)
    {
        CassandraSidecarDaemon app = Guice.createInjector(new MainModule())
                                          .getInstance(CassandraSidecarDaemon.class);

        app.start();
        Runtime.getRuntime().addShutdownHook(new Thread(app::stop));
    }
}

