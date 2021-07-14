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

package org.apache.cassandra.sidecar.cdc;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Inject;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.utils.Pair;

/**
 * Reads a Cassandra commit log. Read offsets are provided by the CdcIndexWatcher
 */
public class CommitLogReader
{

    private static final Logger logger = LoggerFactory.getLogger(CommitLogReader.class);
    private final ExecutorService executor;
    private LinkedBlockingDeque<Pair<Path, Integer>> commitLogReadRequests;
    private int prevOffset = 0;
    private final org.apache.cassandra.db.commitlog.CommitLogReader commitLogReader;
    private Path prevCommitLogFilePath = null;
    private Path prevIndexFilePath = null;
    private final MutationHandler mutationHandler;
    private CDCBookmark bookmark;
    private volatile boolean running;

    // These are copied from CommitLogDescriptor class. Wonders of the current Cass CDC design. (sigh)
    private static final String SEPARATOR = "-";
    private static final String FILENAME_PREFIX = "CommitLog" + SEPARATOR;
    private static final String FILENAME_EXTENSION = ".log";

    private static final String IDX_FILENAME_SUFFIX = "_cdc.idx";
    private static final Pattern COMMIT_LOG_IDX_FILE_PATTERN = Pattern.compile(FILENAME_PREFIX +
            "((\\d+)(" + SEPARATOR + "\\d+)?)" + IDX_FILENAME_SUFFIX);

    @Inject
    CommitLogReader(MutationHandler mutationHandler, CDCBookmark cdcBookmark)
    {
        this.executor = Executors.newSingleThreadExecutor(); //TODO: Single reader, can be multiple readers, but watch
        // for the commit log deletion process, it has to be changed.
        //TODO : currently this is unbounded, do we want to bound it and add alerts?
        this.commitLogReadRequests = new LinkedBlockingDeque<>();
        this.commitLogReader = new org.apache.cassandra.db.commitlog.CommitLogReader();
        this.mutationHandler = mutationHandler;
        this.bookmark = cdcBookmark;
        this.running = true;
    }

    public void start()
    {
        // Before sending out the live change stream, process changes since the last bookmark if there are valid
        // ones.
        this.processOldCommitLogs();

        executor.submit(() ->
        {
            try
            {
                this.bookmark.startBookmarkSync();
                // Process the live change stream.
                while (this.running)
                {
                    logger.debug("Waiting for a new event");
                    this.processCDCWatchEvent(this.commitLogReadRequests.take());
                }
            }
            catch (Throwable e)
            {
                logger.error("Error handling the CDC watch event : {} ", e.getMessage());
            }
            logger.info("Exit processing watch events");
            return;
        });
    }

    public void submitReadRequest(Pair<Path, Integer> idxPath)
    {
        this.commitLogReadRequests.add(idxPath);
    }

    /**
     * Checks whether the given file name is a valid index file. Validate file name against the expected format.
     */
    public static boolean isValidIndexFile(String indexFileName)
    {
        return  ((COMMIT_LOG_IDX_FILE_PATTERN.matcher(indexFileName)).matches());
    }

    /**
     * Construct a CommitLogDescriptor from the index file name.
     */
    public static CommitLogDescriptor fromIndexFile(String indexFileName)
    {
        if (!isValidIndexFile(indexFileName))
        {
            logger.warn("Provided file name [{}] is not an CDC index file", indexFileName);
            return null;
        }
        return CommitLogDescriptor.fromFileName(indexFileName.replace(IDX_FILENAME_SUFFIX, FILENAME_EXTENSION));
    }

    /**
     * Gets a list of commit logs since the persisted bookmark. The ordering defines the sequence
     * they should be processed by the CDC reader.
     *
     * @return ordered list of commit logs since the persisted bookmark.
     */
    public static SortedSet<CommitLogPosition> getSortedCommitLogPositionsSinceCommitLog(
            CommitLogPosition bookmarkedPosition)
    {
        SortedSet<CommitLogPosition> commitLogPositions = new TreeSet<>();
        // If there's no bookmark, this could be the initial run or the bookmark is lost. In either case, we
        // return an empty list of commit logs so caller can take necessary actions.
        if (bookmarkedPosition == null)
        {
            return commitLogPositions;
        }

        SortedSet<CommitLogPosition> allCommitLogs = new TreeSet<>();
        try (DirectoryStream<Path> stream =
                     Files.newDirectoryStream(Paths.get(DatabaseDescriptor.getCDCLogLocation())))
        {
            for (Path path : stream)
            {
                if (!Files.isDirectory(path))
                {
                    Path fileNamePath = path.getFileName();
                    if (fileNamePath == null)
                    {
                        continue;
                    }
                    String fileName = fileNamePath.toString();
                    if (isValidIndexFile(fileName))
                    {
                        allCommitLogs.add(new CommitLogPosition(fromIndexFile(fileName).id, 0));
                    }
                }
            }
        }
        catch (IOException ex)
        {
            logger.error("Error accessing the CDC dir : {}", ex.getMessage());
            return commitLogPositions;
        }

        if (allCommitLogs.size() == 0)
        {
            return commitLogPositions;
        }

        // Use this flag to find the point of bookmark in the list of Idx files
        // that are sorted by the commit log segment ids.
        boolean foundBookmark = false;

        for (CommitLogPosition commitLog : allCommitLogs)
        {
            if (commitLog.segmentId == bookmarkedPosition.segmentId)
            {
                foundBookmark = true;
            }
            if (foundBookmark)
            {
                commitLogPositions.add(new CommitLogPosition(commitLog.segmentId,
                        commitLog.segmentId == bookmarkedPosition.segmentId ? bookmarkedPosition.position : 0));
            }
        }

        return commitLogPositions;
    }

    private void processOldCommitLogs()
    {
        try
        {
            if (this.bookmark.isValidBookmark())
            {
                SortedSet<CommitLogPosition> validCommitLogs = new TreeSet<>(Collections.reverseOrder());
                validCommitLogs.addAll(CommitLogReader.getSortedCommitLogPositionsSinceCommitLog(
                        this.bookmark.getPersistedBookmark()));
                for (CommitLogPosition commitLogPosition : validCommitLogs)
                {

                    this.commitLogReadRequests.addFirst(Pair.create(Paths.get(DatabaseDescriptor.getCDCLogLocation(),
                            new CommitLogDescriptor(commitLogPosition.segmentId, null, null)
                                    .cdcIndexFileName()),
                            commitLogPosition.position));
                }
            }
        }
        catch (Exception ex)
        {
            logger.error("Error when processing commit logs since the last bookmark: {}. Start with a fresh " +
                    "data dump from required tables", ex.getMessage());
        }
    }

    private void processCDCWatchEvent(Pair<Path, Integer> idxPath)
    {
        Path indexFilePath = idxPath.left;
        Integer savedOffset = idxPath.right;

        if (indexFilePath == null)
        {
            logger.error("Index file path is empty.");
            return;
        }

        if (savedOffset > 0)
        {
            prevOffset = savedOffset;
        }

        logger.debug("Processing a commit log segment");

        Path indexFileName = indexFilePath.getFileName();
        if (indexFileName == null || !CommitLogReader.isValidIndexFile(indexFileName.toString()))
        {
            logger.error("File is not an index file : {} ", indexFilePath.toString());
            return;
        }
        String commitLogFile = indexFileName.toString().replace(IDX_FILENAME_SUFFIX, FILENAME_EXTENSION);
        Path parentDir = indexFilePath.getParent();
        if (parentDir == null)
        {
            logger.error("Unable to parse commit log file path from {} ", indexFilePath.toString());
            return;
        }
        Path commitLogPath = Paths.get(parentDir.toString(), commitLogFile);

        try
        {
            int offset;
            long segmentId;
            logger.debug("Opening the idx file {}", indexFilePath.toString());
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(new FileInputStream(indexFilePath.toFile()), Charset.defaultCharset()),
                    128))
            {
                logger.debug("Reader {}", reader);
                offset = Integer.parseInt(reader.readLine());
                logger.debug("Offset is {}", offset);
                segmentId =  CommitLogDescriptor.fromFileName(commitLogFile).id;
                logger.debug("Segment id is {}", segmentId);

                // BEGIN : TODO
                // We are switching to a newer commit log and deleting the last one. We need to ensure the validity of
                // the bookmark so it remains valid if the CDC reader crashes before observing changes from the new
                // commit log. Failing to do so will trigger a fresh snapshot upon restarting the CDC reader.
                // END : TODO
                // TODO : Do this asynchronously, hand over to a background cleaner thread.
                if (this.prevCommitLogFilePath != null &&
                        !this.prevCommitLogFilePath.toString().equals(commitLogPath.toString()))
                {
                    boolean suc = this.prevCommitLogFilePath.toFile().delete();
                    logger.info("{} the old file {}", suc ? "Deleted" : "Could not delete",
                            this.prevCommitLogFilePath.toString());
                    prevOffset = 0;
                }
                if (this.prevIndexFilePath != null &&
                        !this.prevIndexFilePath.toString().equals(indexFilePath.toString()))
                {
                    boolean suc = this.prevIndexFilePath.toFile().delete();
                    logger.info("{} the old CDC idx file {}", suc ? "Deleted" : "Could not delete",
                            this.prevIndexFilePath);
                }
            }
            catch (NumberFormatException ex)
            {
                logger.error("Error when reading offset/segment id from the idx file {} : {}", indexFilePath.toString(),
                        ex.getMessage());
                throw ex;
            }
            catch (Exception ex)
            {
                logger.error("Error when deleting the old file {} : {}", this.prevCommitLogFilePath.toString(),
                        ex.getMessage());
                throw ex;
            }

            logger.info("Reading from the commit log file {} at offset {}", commitLogPath.toString(), prevOffset);
            this.prevCommitLogFilePath = commitLogPath;
            this.prevIndexFilePath = indexFilePath;


            CommitLogPosition clp = new CommitLogPosition(segmentId, prevOffset);
            commitLogReader.readCommitLogSegment(this.mutationHandler, commitLogPath.toFile(), clp, -1,
                    false);
            prevOffset = offset;
        }
        catch (Throwable ex)
        {
            logger.error("Error when processing a commit log segment : {}", ex.getMessage());
        }
        logger.debug("Commit log segment processed.");

    }

    public void stop()
    {
        this.running = false;
        this.executor.shutdown();
        this.mutationHandler.stop();
    }
}
