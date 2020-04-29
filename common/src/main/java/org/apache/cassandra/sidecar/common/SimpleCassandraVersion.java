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

package org.apache.cassandra.sidecar.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Objects;

/**
 * Implements versioning used in Cassandra and CQL.
 * <p>
 * Note: The following code uses a slight variation from the semver document (http://semver.org).
 * </p>
 *
 * The rules here are a bit different than normal semver comparison.  For simplicity,
 * an alpha version of 4.0 or a snapshot is equal to 4.0.  This allows us to test sidecar
 * against alpha versions of a release.
 *
 * While it's possible to implement full version comparison, it's likely not very useful
 * This is because the main testing we are going to do will be against release versions - something like 4.0.
 * We want to list an adapter as being compatible with 4.0 - and that should include 4.0 alpha, etc.
 */
public class SimpleCassandraVersion implements Comparable<SimpleCassandraVersion>
{
    /**
     * note: 3rd group matches to words but only allows number and checked after regexp test.
     * this is because 3rd and the last can be identical.
     **/
    private static final String VERSION_REGEXP = "(\\d+)\\.(\\d+)(?:\\.(\\w+))?(\\-[.\\w]+)?([.+][.\\w]+)?";

    private static final Pattern pattern = Pattern.compile(VERSION_REGEXP);
    private static final Pattern SNAPSHOT = Pattern.compile("-SNAPSHOT");

    public final int major;
    public final int minor;
    public final int patch;

    /**
     * Parse a version from a string.
     *
     * @param version the string to parse
     * @throws IllegalArgumentException if the provided string does not
     *                                  represent a version
     */
    public static SimpleCassandraVersion create(String version)
    {
        String stripped = SNAPSHOT.matcher(version).replaceFirst("");
        Matcher matcher = pattern.matcher(stripped);
        if (!matcher.matches())
            throw new IllegalArgumentException("Invalid version value: " + version);

        try
        {
            int major = Integer.parseInt(matcher.group(1));
            int minor = Integer.parseInt(matcher.group(2));
            int patch = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) : 0;

            return SimpleCassandraVersion.create(major, minor, patch);
        }
        catch (NumberFormatException e)
        {
            throw new IllegalArgumentException("Invalid version value: " + version, e);
        }
    }

    public static SimpleCassandraVersion create(int major, int minor, int patch)
    {
        if (major < 0 || minor < 0 || patch < 0)
        {
            throw new IllegalArgumentException();
        }
        return new SimpleCassandraVersion(major, minor, patch);
    }

    public static SimpleCassandraVersion create(ICassandraFactory factory)
    {
        return SimpleCassandraVersion.create(factory.getClass().getAnnotation(MinimumVersion.class).value());
    }

    private SimpleCassandraVersion(int major, int minor, int patch)
    {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
    }


    public int compareTo(SimpleCassandraVersion other)
    {
        if (major < other.major)
            return -1;
        if (major > other.major)
            return 1;

        if (minor < other.minor)
            return -1;
        if (minor > other.minor)
            return 1;

        if (patch < other.patch)
            return -1;
        if (patch > other.patch)
            return 1;
        return 0;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof SimpleCassandraVersion))
            return false;
        SimpleCassandraVersion that = (SimpleCassandraVersion) o;
        return major == that.major
                && minor == that.minor
                && patch == that.patch;
    }

    /**
     * Returns true if this > v2
     * @param v2
     * @return
     */
    public boolean isGreaterThan(SimpleCassandraVersion v2)
    {
        return compareTo(v2) > 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(major, minor, patch);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(major).append('.').append(minor).append('.').append(patch);

        return sb.toString();
    }
}
