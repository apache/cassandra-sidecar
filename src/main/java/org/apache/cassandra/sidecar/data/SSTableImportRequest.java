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

package org.apache.cassandra.sidecar.data;

import java.util.Objects;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.data.SSTableUploads;

import static org.apache.cassandra.sidecar.utils.RequestUtils.parseBooleanQueryParam;

/**
 * Holder class for the {@code org.apache.cassandra.sidecar.routes.SSTableUploadsResource}
 * request parameters
 */
public class SSTableImportRequest extends SSTableUploads
{
    private final boolean resetLevel;
    private final boolean clearRepaired;
    private final boolean verifySSTables;
    private final boolean verifyTokens;
    private final boolean invalidateCaches;
    private final boolean extendedVerify;
    private final boolean copyData;

    /**
     * Constructs an SSTableImportRequest
     *
     * @param qualifiedTableName the qualified table name in Cassandra
     * @param uploadId           an identifier for the upload
     * @param resetLevel         if the level should be reset to 0 on the new SSTables
     * @param clearRepaired      if repaired info should be wiped from the new SSTables
     * @param verifySSTables     if the new SSTables should be verified that they are not corrupt
     * @param verifyTokens       if the tokens in the new SSTables should be verified that they are owned by the
     *                           current node
     * @param invalidateCaches   if row cache should be invalidated for the keys in the new SSTables
     * @param extendedVerify     if we should run an extended verify checking all values in the new SSTables
     * @param copyData           if we should copy data from source paths instead of moving them
     */
    public SSTableImportRequest(QualifiedTableName qualifiedTableName, String uploadId, boolean resetLevel,
                                boolean clearRepaired, boolean verifySSTables, boolean verifyTokens,
                                boolean invalidateCaches, boolean extendedVerify, boolean copyData)
    {
        super(qualifiedTableName, uploadId);
        this.resetLevel = resetLevel;
        this.clearRepaired = clearRepaired;
        this.verifySSTables = verifySSTables;
        this.verifyTokens = verifyTokens;
        this.invalidateCaches = invalidateCaches;
        this.extendedVerify = extendedVerify;
        this.copyData = copyData;
    }

    /**
     * @return true if the level should be reset on the new SSTables, false otherwise
     */
    public boolean resetLevel()
    {
        return resetLevel;
    }

    /**
     * @return true if repaired info should be kept from the new SSTables, false otherwise
     */
    public boolean clearRepaired()
    {
        return clearRepaired;
    }

    /**
     * @return true if the new SSTables should be verified that they are not corrupt, false otherwise
     */
    public boolean verifySSTables()
    {
        return verifySSTables;
    }

    /**
     * @return true if the tokens in the new SSTables should be verified that they are owned by the current node,
     * false otherwise
     */
    public boolean verifyTokens()
    {
        return verifyTokens;
    }

    /**
     * @return true if row cache should be invalidated for the keys in the new SSTables, false otherwise
     */
    public boolean invalidateCaches()
    {
        return invalidateCaches;
    }

    /**
     * @return true if we should run an extended verify checking all values in the new SSTables, false otherwise
     */
    public boolean extendedVerify()
    {
        return extendedVerify;
    }

    /**
     * @return true if we should copy data from source paths instead of moving them, false otherwise
     */
    public boolean copyData()
    {
        return copyData;
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SSTableImportRequest that = (SSTableImportRequest) o;
        return resetLevel == that.resetLevel
               && clearRepaired == that.clearRepaired
               && verifySSTables == that.verifySSTables
               && verifyTokens == that.verifyTokens
               && invalidateCaches == that.invalidateCaches
               && extendedVerify == that.extendedVerify
               && copyData == that.copyData
               && uploadId().equals(that.uploadId())
               && keyspace().equals(that.keyspace())
               && table().equals(that.table());
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode()
    {
        return Objects.hash(uploadId(), keyspace(), table(), resetLevel, clearRepaired, verifySSTables,
                            verifyTokens, invalidateCaches, extendedVerify, copyData);
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "SSTableUploadRequest{" +
               "uploadId='" + uploadId() + '\'' +
               ", keyspace='" + keyspace() + '\'' +
               ", tableName='" + table() + '\'' +
               ", resetLevel=" + resetLevel +
               ", clearRepaired=" + clearRepaired +
               ", verifySSTables=" + verifySSTables +
               ", verifyTokens=" + verifyTokens +
               ", invalidateCaches=" + invalidateCaches +
               ", extendedVerify=" + extendedVerify +
               ", copyData=" + copyData +
               '}';
    }

    /**
     * Returns a new instance of the {@link SSTableImportRequest} built from the {@link RoutingContext context}.
     *
     * @param qualifiedTableName the qualified table name in Cassandra
     * @param context context from handler
     * @return SSTableImportRequest created from params
     */
    public static SSTableImportRequest from(QualifiedTableName qualifiedTableName, RoutingContext context)
    {
        HttpServerRequest request = context.request();
        return new SSTableImportRequest(qualifiedTableName,
                                        context.pathParam("uploadId"),
                                        parseBooleanQueryParam(request, "resetLevel", true),
                                        parseBooleanQueryParam(request, "clearRepaired", true),
                                        parseBooleanQueryParam(request, "verifySSTables", true),
                                        parseBooleanQueryParam(request, "verifyTokens", true),
                                        parseBooleanQueryParam(request, "invalidateCaches", true),
                                        parseBooleanQueryParam(request, "extendedVerify", true),
                                        parseBooleanQueryParam(request, "copyData", false));
    }
}
