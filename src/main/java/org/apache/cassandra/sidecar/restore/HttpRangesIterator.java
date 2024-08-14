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

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.cassandra.sidecar.common.utils.HttpRange;
import org.apache.cassandra.sidecar.common.utils.Preconditions;

/**
 * Iterator over the produced http ranges
 */
public class HttpRangesIterator implements Iterator<HttpRange>
{
    private final long totalBytes;
    private final int rangeSize;
    private long offset = 0;

    public HttpRangesIterator(long totalBytes, int rangeSize)
    {
        Preconditions.checkArgument(totalBytes > 0, "totalBytes must be positive");
        Preconditions.checkArgument(rangeSize > 0, "rangeSize must be positive");
        this.totalBytes = totalBytes;
        this.rangeSize = rangeSize;
    }

    @Override
    public boolean hasNext()
    {
        return offset < totalBytes;
    }

    @Override
    public HttpRange next()
    {
        if (!hasNext())
        {
            throw new NoSuchElementException("No more HttpRanges");
        }
        long end = Math.min(offset + rangeSize, totalBytes) - 1;
        HttpRange range = HttpRange.of(offset, end);
        offset = end + 1;
        return range;
    }
}
