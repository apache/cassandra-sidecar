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

package org.apache.cassandra.sidecar.routes;

import java.util.function.Supplier;
import javax.annotation.Nullable;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.ConnectionException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.cassandra.sidecar.CQLSession;

/**
 * Basic health check to verify that a CQL connection can be established with a basic SELECT query.
 */
@Singleton
public class HealthCheck implements Supplier<Boolean>
{
    private static final Logger logger = LoggerFactory.getLogger(HealthCheck.class);

    @Nullable
    private final CQLSession session;

    @Inject
    public HealthCheck(@Nullable CQLSession session)
    {
        this.session = session;
    }

    /**
     * The actual health check
     *
     * @return
     */
    private boolean check()
    {
        try
        {
            if (session != null && session.getLocalCql() != null)
            {
                ResultSet rs = session.getLocalCql().execute("SELECT release_version FROM system.local");
                boolean result = (rs.one() != null);
                logger.debug("HealthCheck status: {}", result);
                return result;
            }
        }
        catch (NoHostAvailableException | ConnectionException nha)
        {
            logger.trace("NoHostAvailableException in HealthCheck - Cassandra Down");
        }
        catch (Exception e)
        {
            logger.error("Failed to reach Cassandra.", e);
        }
        return false;
    }

    /**
     * Get the check value
     *
     * @return true or false based on whether check was successful
     */
    @Override
    public Boolean get()
    {
        return check();
    }

}
