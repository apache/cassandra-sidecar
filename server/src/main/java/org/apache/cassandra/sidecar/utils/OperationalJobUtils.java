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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;
import org.apache.cassandra.sidecar.job.OperationalJob;

import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.FAILED;

/**
 * Utility class for OperationalJob framework operations.
 */
public class OperationalJobUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OperationalJobUtils.class);

    /**
     * In the operational job context, sends a {@link OperationalJobResponse} based on the status of the job.
     *
     * @param context the request context
     * @param job     the operational job to reports status on
     */
    public static void sendStatusBasedResponse(RoutingContext context, OperationalJob job)
    {
        OperationalJobStatus status = job.status();
        LOGGER.info("Job completion status={} jobId={}", status, job.jobId());
        if (status.isCompleted())
        {
            context.response().setStatusCode(HttpResponseStatus.OK.code());
        }
        else
        {
            context.response().setStatusCode(HttpResponseStatus.ACCEPTED.code());
        }

        String reason = status == FAILED ? job.asyncResult().cause().getMessage() : null;
        context.json(new OperationalJobResponse(job.jobId(), status, job.name(), reason));
    }
}
