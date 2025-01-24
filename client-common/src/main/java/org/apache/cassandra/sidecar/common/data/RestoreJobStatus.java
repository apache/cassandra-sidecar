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

package org.apache.cassandra.sidecar.common.data;

/**
 * Holds all possible restore job statuses
 */
public enum RestoreJobStatus
{
    /**
     * The external controller creates the RestoreJob
     */
    CREATED,
    /**
     * The external controller updates the status of the RestoreJob to STAGE_READY.
     * It indicates that all relevant slices of the RestoreJob have been uploaded and ready to be consumed.
     */
    STAGE_READY,
    /**
     * All relevant slices are staged and the staged data satisfies the consistency requirement of the RestoreJob.
     * The external controller updates the status of the RestoreJob to STAGED.
     */
    STAGED,
    /**
     * The external controller updates the status of the RestoreJob to IMPORT_READY
     * It indicates that all staged data now are ready to be imported into Cassandra.
     */
    IMPORT_READY,
    /**
     * @deprecated replaced by {@link #ABORTED}
     */
    FAILED,
    /**
     * The external controller aborts the RestoreJob due to some failure, e.g. consistency not satisfied, timeout, etc.
     * The RestoreJob can also be aborted by Sidecar, if and only if, the external controller fails to renew the lease for certain long duration.
     * Such RestoreJobs are aborted due to expiry.
     */
    ABORTED,
    /**
     * The external controller marks the RestoreJob as succeeded.
     */
    SUCCEEDED;

    public boolean isFinal()
    {
        return this == FAILED || this == ABORTED || this == SUCCEEDED;
    }
}
