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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class with utility methods for CDC.
 */
public final class CdcUtil
{
    private static final String SEPARATOR = "-";
    private static final String FILENAME_PREFIX = "CommitLog" + SEPARATOR;
    private static final String LOG_FILE_EXTENSION = ".log";
    private static final String IDX_FILE_EXTENSION = "_cdc.idx";
    private static final int LOG_FILE_EXTENSION_LENGTH = LOG_FILE_EXTENSION.length();
    private static final int IDX_FILE_EXTENSION_LENGTH = IDX_FILE_EXTENSION.length();
    private static final String LOG_FILE_COMPLETE_INDICATOR = "COMPLETED";
    private static final String FILENAME_EXTENSION = "(" + IDX_FILE_EXTENSION + "|" + LOG_FILE_EXTENSION + ")";
    static final Pattern SEGMENT_PATTERN = Pattern.compile(FILENAME_PREFIX + "(?:\\d+" + SEPARATOR + ")?" + "(\\d+)" + FILENAME_EXTENSION);
    public static final Pattern IDX_FILE_PATTERN = Pattern.compile(FILENAME_PREFIX + "(?:\\d+" + SEPARATOR + ")?" + "(\\d+)" + IDX_FILE_EXTENSION);

    private static final int READ_INDEX_FILE_MAX_RETRY = 5;

    private CdcUtil()
    {
        throw new UnsupportedOperationException("Do not instantiate.");
    }

    public static String getIdxFilePrefix(String idxFileName)
    {
        return idxFileName.substring(0, idxFileName.length() - IDX_FILE_EXTENSION_LENGTH);
    }

    public static String getLogFilePrefix(String logFileName)
    {
        return logFileName.substring(0, logFileName.length() - LOG_FILE_EXTENSION_LENGTH);
    }

    public static String getIdxFileName(String logFileName)
    {
        return logFileName.replace(LOG_FILE_EXTENSION, IDX_FILE_EXTENSION);
    }

    public static File getIdxFile(File logFile)
    {
        return new File(logFile.getParent(), getIdxFileName(logFile.getName()));
    }

    public static String getLogFileName(String indexFileName)
    {
        return indexFileName.replace(IDX_FILE_EXTENSION, LOG_FILE_EXTENSION);
    }

    public static CdcIndex parseIndexFile(File indexFile, long segmentFileLength) throws IOException
    {
        List<String> lines = null;
        // For an index file, if it exists, it should have non-empty content.
        // Therefore, the lines read from the file should not be empty.
        // If it is empty, retry reading the file. The cause is the contention between the reader and the index file writer.
        // In most case, the loop should only run once.
        for (int i = 0; i < READ_INDEX_FILE_MAX_RETRY; i++)
        {
            try
            {
                lines = Files.readAllLines(indexFile.toPath());
                if (!lines.isEmpty())
                    break;
            }
            catch (IOException e)
            {
                throw new IOException("Unable to parse the CDC segment index file " + indexFile.getName(), e);
            }
        }
        if (lines.isEmpty())
        {
            throw new IOException("Unable to read anything from the CDC segment index file " + indexFile.getName());
        }

        final String lastLine = lines.get(lines.size() - 1);
        final boolean isCompleted = lastLine.equals(LOG_FILE_COMPLETE_INDICATOR);
        final long latestPosition = isCompleted ? segmentFileLength : Long.parseLong(lastLine);
        return new CdcIndex(latestPosition, isCompleted);
    }

    /**
     * @param idxFileName Commit log segment idx filename
     * @return log segment filename for associated idx file
     */
    public static String idxToLogFileName(String idxFileName)
    {
        return idxFileName.substring(0, idxFileName.length() - IDX_FILE_EXTENSION.length()) + LOG_FILE_EXTENSION;
    }

    public static long parseSegmentId(String name)
    {
        final Matcher matcher = SEGMENT_PATTERN.matcher(name);
        if (matcher.matches())
        {
            return Long.parseLong(matcher.group(1));
        }
        else
        {
            throw new IllegalStateException("Invalid CommitLog name: " + name);
        }
    }

    /**
     * Class representing Cdc index
     */
    public static class CdcIndex
    {
        public final long latestFlushPosition;
        public final boolean isCompleted;

        public CdcIndex(long latestFlushPosition, boolean isCompleted)
        {
            this.latestFlushPosition = latestFlushPosition;
            this.isCompleted = isCompleted;
        }
    }

    /**
     * Validate for the cdc (log or index) file name.see {@link SEGMENT_PATTERN} for the format
     *
     * @param fileName name of the file
     * @return true if the name is valid; otherwise, false
     */
    public static boolean isValid(String fileName)
    {
        return SEGMENT_PATTERN.matcher(fileName).matches();
    }

    public static boolean isLogFile(String fileName)
    {
        return isValid(fileName) && fileName.endsWith(LOG_FILE_EXTENSION);
    }

    /**
     * @param idxFileName name of the file.
     * @return true if the filename is a valid cdc_raw log segment idx file
     */
    public static boolean isValidIdxFile(String idxFileName)
    {
        return IDX_FILE_PATTERN.matcher(idxFileName).matches();
    }

    public static boolean isIndexFile(String fileName)
    {
        return isValid(fileName) && matchIndexExtension(fileName);
    }

    public static boolean matchIndexExtension(String fileName)
    {
        return fileName.endsWith(IDX_FILE_EXTENSION);
    }
}
