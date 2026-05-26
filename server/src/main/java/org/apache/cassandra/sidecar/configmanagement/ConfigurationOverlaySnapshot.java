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

package org.apache.cassandra.sidecar.configmanagement;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Objects;

import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;

/**
 * Represents a snapshot of a configuration overlay with its metadata.
 * The SHA-256 hash is dynamically computed from the overlay contents and cached.
 */
public class ConfigurationOverlaySnapshot
{
    @NotNull
    private final Instant lastModified;

    @NotNull
    private final CassandraConfigurationOverlay overlay;

    private volatile String hash;

    public ConfigurationOverlaySnapshot(@NotNull Instant lastModified,
                                        @NotNull CassandraConfigurationOverlay overlay)
    {
        this.lastModified = Objects.requireNonNull(lastModified, "lastModified must not be null");
        this.overlay = Objects.requireNonNull(overlay, "overlay must not be null");
    }

    /**
     * Returns the SHA-256 hash of the overlay contents, prefixed with "sha256:".
     * Computed on first access and cached for subsequent calls.
     *
     * @return the content hash in the form "sha256:&lt;64 hex chars&gt;"
     */
    @NotNull
    public String hash()
    {
        if (hash == null)
        {
            hash = computeHash();
        }
        return hash;
    }

    @NotNull
    public Instant lastModified()
    {
        return lastModified;
    }

    @NotNull
    public CassandraConfigurationOverlay overlay()
    {
        return overlay;
    }

    private String computeHash()
    {
        try
        {
            byte[] bytes = overlay.toJson().toBuffer().getBytes();
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(bytes);
            return "sha256:" + bytesToHex(hashBytes);
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException("Failed to compute configuration hash", e);
        }
    }

    private static String bytesToHex(byte[] bytes)
    {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes)
        {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        ConfigurationOverlaySnapshot that = (ConfigurationOverlaySnapshot) o;
        return Objects.equals(lastModified, that.lastModified)
               && Objects.equals(overlay, that.overlay);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lastModified, overlay);
    }

    @Override
    public String toString()
    {
        return new JsonObject()
               .put("hash", hash())
               .put("lastModified", lastModified.toString())
               .put("overlay", overlay.toJson())
               .encodePrettily();
    }
}
