package org.apache.cassandra.sidecar.cdc;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.sidecar.Configuration;

/**
 * Manages the CDC reader bookmark. This tracks the last successfully processed offset
 * of a commit log.
 */
@Singleton
public class CDCBookmark extends TimerTask
{
    // Tracks last disk sync'd commit log position.
    private CommitLogPosition lastSyncedPosition;
    // Tracks last successfully processed commit log position by the CDC reader.
    private CommitLogPosition lastProcessedPosition;
    private final Timer timer;
    private static final String BOOKMARK = "CdcReader.bookmark";
    private static final Logger logger = LoggerFactory.getLogger(CDCBookmark.class);
    private final ReentrantLock bookmarkLock = new ReentrantLock();
    private final Configuration conf;

    @Inject
    CDCBookmark(Configuration conf)
    {
        this.lastSyncedPosition = null;
        this.lastProcessedPosition = null;
        this.conf = conf;
        this.timer = new Timer();
    }

    /**
     * Persists last successfully processed commit log offset to the disk.
     */
    public void syncBookmark()
    {
        CommitLogPosition lastPosition = this.getLastProcessedPosition();

        if (lastPosition == null)
        {
            return;
        }
        logger.debug("Last processed bookmark {}", this.lastProcessedPosition.toString());
        try
        {
            if (lastPosition.equals(this.lastSyncedPosition))
            {
                return;
            }

            CommitLogPosition.CommitLogPositionSerializer serializer =
                    new CommitLogPosition.CommitLogPositionSerializer();

            // TODO: JSON ser-de and write-rename instead of writing directly to the bookmark
            try (FileOutputStream fileOutputStream = new FileOutputStream(
                    new File(this.getBookmarkPath())))
            {
                DataOutputPlus outBuffer = new DataOutputBuffer();
                serializer.serialize(lastPosition, outBuffer);
                fileOutputStream.write(((DataOutputBuffer) outBuffer).getData());
                fileOutputStream.flush();
                this.lastSyncedPosition = lastPosition;
                logger.info("Successfully synced bookmark {} to the file {}", this.lastSyncedPosition.toString(),
                        this.getBookmarkPath());
            }
            catch (IOException e)
            {
                logger.error("Error when writing bookmark {} to the file {}", lastPosition.toString(),
                        this.getBookmarkPath());
            }
        }
        catch (Exception ex)
        {
            logger.error("Sync exception {}", ex.getMessage());
        }
    }

    /**
     * Gets the path to the CDC reader bookmark.
     *
     * @return complete path to the bookmark file.
     */
    public String getBookmarkPath()
    {
        return String.format("%s/%s", DatabaseDescriptor.getCDCLogLocation(),
                BOOKMARK);
    }

    @Override
    public void run()
    {
        this.syncBookmark();
    }

    /**
     * Gets the last successfully processed commit log offset.
     * This method is thread safe.
     *
     * @return last successfully processed commit log offset.
     */
    public CommitLogPosition getLastProcessedPosition()
    {
        CommitLogPosition lastPosition = null;
        try
        {
            bookmarkLock.lock();
            if (this.lastProcessedPosition != null)
            {
                lastPosition = new CommitLogPosition(this.lastProcessedPosition.segmentId,
                        this.lastProcessedPosition.position);

            }
        }
        finally
        {
            bookmarkLock.unlock();
        }
        return lastPosition;
    }

    /**
     * Sets the last successfully processed commit log offset.
     * This method is thread safe.
     *
     */
    public void setLastProcessedPosition(CommitLogPosition processedPosition)
    {
        try
        {
            bookmarkLock.lock();
            this.lastProcessedPosition = processedPosition;
        }
        finally
        {
            bookmarkLock.unlock();
        }
    }

    /**
     * Starts the background thread to write processed commit log positions to the disk.
     * */
    public void startBookmarkSync()
    {
        timer.schedule(this, 0, DatabaseDescriptor.getCommitLogSyncPeriod());
    }

    /**
     * Gets the persisted commit log offset from the bookmark on the disk.
     *
     * @return persisted commit log offset.
     */
    public CommitLogPosition getPersistedBookmark()
    {
        CommitLogPosition.CommitLogPositionSerializer serializer =
                new CommitLogPosition.CommitLogPositionSerializer();
        try (FileInputStream fileInputStream =
                     new FileInputStream(new File(this.getBookmarkPath())))
        {
            DataInputPlus inputBuffer = new DataInputPlus.DataInputStreamPlus(fileInputStream);
            return serializer.deserialize(inputBuffer);
        }
        catch (IOException ex)
        {
            logger.error("Error when reading the saved bookmark {}", this.getBookmarkPath());
            return null;
        }
    }

    /**
     * Checks whether there's a valid persisted bookmark.
     *
     * @return whether there's a valid bookmark.
     */
    public boolean isValidBookmark()
    {
        CommitLogPosition bookmark = getPersistedBookmark();
        if (bookmark == null)
        {
            return false;
        }
        // It's fine for compression to be null as we are not using this CommitLogDescriptor to write commit logs.
        CommitLogDescriptor commitLogDescriptor = new CommitLogDescriptor(bookmark.segmentId, null,
                null);

        if (commitLogDescriptor == null ||
                !Paths.get(DatabaseDescriptor.getCDCLogLocation(),
                        commitLogDescriptor.cdcIndexFileName()).toFile().exists() ||
                !Paths.get(DatabaseDescriptor.getCDCLogLocation(),
                        commitLogDescriptor.fileName()).toFile().exists())
        {
            return false;
        }
        return true;
    }
}
