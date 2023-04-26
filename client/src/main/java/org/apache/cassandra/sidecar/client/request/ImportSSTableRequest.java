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

package org.apache.cassandra.sidecar.client.request;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.data.SSTableImportResponse;

/**
 * Represents a request to import SSTable components previously uploaded for an upload identifier
 */
public class ImportSSTableRequest extends DecodableRequest<SSTableImportResponse>
{
    /**
     * Constructs a decodable request with the provided {@code requestURI}
     *
     * @param keyspace      the keyspace in Cassandra
     * @param tableName     the table name in Cassandra
     * @param uploadId      an identifier for the upload
     * @param importOptions additional options for the import process
     */
    public ImportSSTableRequest(String keyspace, String tableName, String uploadId, ImportOptions importOptions)
    {
        super(requestURI(keyspace, tableName, uploadId, importOptions));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HttpMethod method()
    {
        return HttpMethod.PUT;
    }

    /**
     * Options for the SSTable import
     */
    public static class ImportOptions
    {
        private Boolean resetLevel;
        private Boolean clearRepaired;
        private Boolean verifySSTables;
        private Boolean verifyTokens;
        private Boolean invalidateCaches;
        private Boolean extendedVerify;
        private Boolean copyData;

        public ImportOptions()
        {
        }

        /**
         * Sets the {@code resetLevel} and returns a reference to this Builder enabling method chaining.
         *
         * @param resetLevel the {@code resetLevel} to set
         * @return a reference to this Builder
         */
        public ImportOptions resetLevel(boolean resetLevel)
        {
            this.resetLevel = resetLevel;
            return this;
        }

        /**
         * Sets the {@code clearRepaired} and returns a reference to this ImportOptions enabling method chaining.
         *
         * @param clearRepaired the {@code clearRepaired} to set
         * @return a reference to this ImportOptions
         */
        public ImportOptions clearRepaired(boolean clearRepaired)
        {
            this.clearRepaired = clearRepaired;
            return this;
        }

        /**
         * Sets the {@code verifySSTables} and returns a reference to this ImportOptions enabling method chaining.
         *
         * @param verifySSTables the {@code verifySSTables} to set
         * @return a reference to this ImportOptions
         */
        public ImportOptions verifySSTables(boolean verifySSTables)
        {
            this.verifySSTables = verifySSTables;
            return this;
        }

        /**
         * Sets the {@code verifyTokens} and returns a reference to this ImportOptions enabling method chaining.
         *
         * @param verifyTokens the {@code verifyTokens} to set
         * @return a reference to this ImportOptions
         */
        public ImportOptions verifyTokens(boolean verifyTokens)
        {
            this.verifyTokens = verifyTokens;
            return this;
        }

        /**
         * Sets the {@code invalidateCaches} and returns a reference to this ImportOptions enabling method chaining.
         *
         * @param invalidateCaches the {@code invalidateCaches} to set
         * @return a reference to this ImportOptions
         */
        public ImportOptions invalidateCaches(boolean invalidateCaches)
        {
            this.invalidateCaches = invalidateCaches;
            return this;
        }

        /**
         * Sets the {@code extendedVerify} and returns a reference to this ImportOptions enabling method chaining.
         *
         * @param extendedVerify the {@code extendedVerify} to set
         * @return a reference to this ImportOptions
         */
        public ImportOptions extendedVerify(boolean extendedVerify)
        {
            this.extendedVerify = extendedVerify;
            return this;
        }

        /**
         * Sets the {@code copyData} and returns a reference to this ImportOptions enabling method chaining.
         *
         * @param copyData the {@code copyData} to set
         * @return a reference to this ImportOptions
         */
        public ImportOptions copyData(boolean copyData)
        {
            this.copyData = copyData;
            return this;
        }
    }

    static String requestURI(String keyspace, String tableName, String uploadId, ImportOptions importOptions)
    {
        String requestUri = ApiEndpointsV1.SSTABLE_IMPORT_ROUTE
                            .replaceAll(ApiEndpointsV1.UPLOAD_ID_PATH_PARAM, uploadId)
                            .replaceAll(ApiEndpointsV1.KEYSPACE_PATH_PARAM, keyspace)
                            .replaceAll(ApiEndpointsV1.TABLE_PATH_PARAM, tableName);

        List<String> options = selectedOptions(importOptions);

        if (options.isEmpty())
        {
            return requestUri;
        }

        return requestUri + options.stream()
                                   .collect(Collectors.joining("&", "?", ""));
    }

    private static List<String> selectedOptions(ImportOptions importOptions)
    {
        List<String> options = new ArrayList<>();

        if (importOptions.resetLevel != null)
        {
            options.add("resetLevel=" + importOptions.resetLevel);
        }
        if (importOptions.clearRepaired != null)
        {
            options.add("clearRepaired=" + importOptions.clearRepaired);
        }
        if (importOptions.verifySSTables != null)
        {
            options.add("verifySSTables=" + importOptions.verifySSTables);
        }
        if (importOptions.verifyTokens != null)
        {
            options.add("verifyTokens=" + importOptions.verifyTokens);
        }
        if (importOptions.invalidateCaches != null)
        {
            options.add("invalidateCaches=" + importOptions.invalidateCaches);
        }
        if (importOptions.extendedVerify != null)
        {
            options.add("extendedVerify=" + importOptions.extendedVerify);
        }
        if (importOptions.copyData != null)
        {
            options.add("copyData=" + importOptions.copyData);
        }

        return options;
    }
}
