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

package org.apache.cassandra.sidecar.restore;

import org.apache.cassandra.sidecar.common.data.ConsistencyVerificationResult;
import org.apache.cassandra.sidecar.common.data.RestoreJobProgressFetchPolicy;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreRange;

/**
 * A factory to create {@link RestoreJobProgressCollector}
 */
public class RestoreJobProgressCollectors
{
    private RestoreJobProgressCollectors()
    {
        throw new UnsupportedOperationException("Initialization is not allowed");
    }

    /**
     * Create a {@link RestoreJobProgressCollector} for the {@link RestoreJob} using the specified {@link RestoreJobProgressFetchPolicy}
     * @param restoreJob restore job to check
     * @param fetchPolicy the policy on compose progress, see {@link RestoreJobProgressFetchPolicy} for detail
     * @return collector
     */
    public static RestoreJobProgressCollector create(RestoreJob restoreJob, RestoreJobProgressFetchPolicy fetchPolicy)
    {
        switch (fetchPolicy)
        {
            case ALL:
                return new CollectAll(restoreJob);
            case FIRST_FAILED:
                return new CollectFirstFailed(restoreJob);
            case ALL_FAILED_AND_PENDING:
                return new CollectAllFailedAndPending(restoreJob);
            default:
                throw new IllegalStateException("Encountered unknown fetch policy: " + fetchPolicy);
        }
    }

    private abstract static class BaseCollector implements RestoreJobProgressCollector
    {
        private final RestoreJobProgress.Builder progressBuilder;
        protected boolean seenFailed = false;
        protected boolean seenPending = false;

        protected BaseCollector(RestoreJob restoreJob)
        {
            progressBuilder = new RestoreJobProgress.Builder(restoreJob);
        }

        protected boolean shouldSkip(ConsistencyVerificationResult checkResult)
        {
            return false;
        }

        @Override
        public void collect(RestoreRange range, ConsistencyVerificationResult checkResult)
        {
            if (checkResult == ConsistencyVerificationResult.FAILED)
            {
                seenFailed = true;
            }
            else if (checkResult == ConsistencyVerificationResult.PENDING)
            {
                seenPending = true;
            }

            if (shouldSkip(checkResult))
            {
                return;
            }

            switch (checkResult)
            {
                case FAILED:
                    progressBuilder.addFailedRange(range);
                    break;
                case PENDING:
                    progressBuilder.addPendingRange(range);
                    break;
                case SATISFIED:
                    progressBuilder.addSucceededRange(range);
                    break;
            }
        }

        @Override
        public RestoreJobProgress toRestoreJobProgress()
        {
            ConsistencyVerificationResult overallStatus = determineOverallStatus();
            return progressBuilder.withOverallStatus(overallStatus)
                                  .build();
        }

        private ConsistencyVerificationResult determineOverallStatus()
        {
            if (seenFailed)
            {
                return ConsistencyVerificationResult.FAILED;
            }
            else if (seenPending)
            {
                return ConsistencyVerificationResult.PENDING;
            }
            else
            {
                return ConsistencyVerificationResult.SATISFIED;
            }
        }
    }

    private static class CollectAll extends BaseCollector
    {
        CollectAll(RestoreJob restoreJob)
        {
            super(restoreJob);
        }

        @Override
        public boolean canCollectMore()
        {
            // visit all ranges
            return true;
        }
    }

    private static class CollectFirstFailed extends BaseCollector
    {
        CollectFirstFailed(RestoreJob restoreJob)
        {
            super(restoreJob);
        }

        @Override
        public boolean canCollectMore()
        {
            // visit ranges until have seen failed or pending
            return !seenFailed;
        }

        @Override
        protected boolean shouldSkip(ConsistencyVerificationResult checkResult)
        {
            // do not collect the ranges that have satisfied
            return checkResult == ConsistencyVerificationResult.SATISFIED || checkResult == ConsistencyVerificationResult.PENDING;
        }
    }

    private static class CollectAllFailedAndPending extends BaseCollector
    {
        protected CollectAllFailedAndPending(RestoreJob restoreJob)
        {
            super(restoreJob);
        }

        @Override
        public boolean canCollectMore()
        {
            // visit all ranges
            return true;
        }

        @Override
        protected boolean shouldSkip(ConsistencyVerificationResult checkResult)
        {
            // do not collect the ranges that have satisfied
            return checkResult == ConsistencyVerificationResult.SATISFIED;
        }
    }
}
