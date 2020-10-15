package org.apache.cassandra.sidecar.cdc;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Inject;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.sidecar.Configuration;
import org.apache.cassandra.sidecar.cdc.output.Output;
import org.apache.cassandra.sidecar.metrics.cdc.CDCReaderMonitor;
/**
 * Implements Cassandra CommitLogReadHandler, dandles mutations read from Cassandra commit logs.
 */
public class MutationHandler implements CommitLogReadHandler
{
    private static final Logger logger = LoggerFactory.getLogger(MutationHandler.class);
    private Output output;
    Future<CommitLogPosition> mutationFuture = null;
    private ExecutorService executor;
    private CDCReaderMonitor monitor;
    private CDCBookmark bookmark;

    @Inject
    public MutationHandler(Configuration conf, CDCReaderMonitor monitor, CDCBookmark cdcBookmark, Output output)
    {
        this.output = output;
        this.executor = Executors.newSingleThreadExecutor();
        this.monitor = monitor;
        this.bookmark = cdcBookmark;
    }

    @Override
    public boolean shouldSkipSegmentOnError(CommitLogReadException exception) throws IOException
    {
        return false;
    }

    @Override
    public void handleUnrecoverableError(CommitLogReadException exception) throws IOException
    {
    }

    @Override
    public void handleMutation(Mutation mutation, int size, int entryLocation, CommitLogDescriptor desc)
    {
        if (mutation == null || !mutation.trackedByCDC())
        {
            return;
        }

        if (output == null)
        {
            logger.error("Output is not initialized");
        }

        logger.debug("Started handling a mutation of the keyspace : {} at offset {}", mutation.getKeyspaceName(),
                entryLocation);

        // Pipeline Mutation reading and de-serializing with sending to the output.
        //TODO: Multiple threads can process Mutations in parallel; hence use a thread pool. Be careful to design
        // bookmarks and commit log deletion to work with multiple threads. Also benchmark the CPU usage of a pool.
        if (mutationFuture != null)
        {
            try
            {
                CommitLogPosition completedPosition = mutationFuture.get();
                logger.debug("Completed sending data at offset {} : {}", completedPosition.segmentId,
                        completedPosition.position);
                this.bookmark.setLastProcessedPosition(completedPosition);
                this.monitor.incSentSuccess();
            }
            //TODO: Re-try logic at the mutation level, with exponential backoff and alerting
            catch (Exception e)
            {
                logger.error("Error sending data at offset {} : {}", e.getMessage());
                this.monitor.incSentFailure();
            }
        }
        mutationFuture = executor.submit(() ->
        {
            CommitLogPosition commitLogPosition = new CommitLogPosition(desc.id, entryLocation);
            for (PartitionUpdate partitionUpdate : mutation.getPartitionUpdates())
            {
                // TODO: bounded number of retries at the partition level.
                try
                {
                    output.emitChange(new Change(PayloadType.PARTITION_UPDATE, MessagingService.current_version,
                            Change.CHANGE_EVENT, partitionUpdate));
                }
                catch (Exception ex)
                {
                    logger.error("Error when sending data at the offset : {}", ex.getMessage());
                    throw ex;
                }
            }
            logger.debug("Done sending data at offset {}", commitLogPosition.position);
            return commitLogPosition;
        });
    }

    public void stop()
    {
        try
        {
            this.output.close();
        }
        catch (IOException ioex)
        {
            logger.error("Error when closing the Output : {}", ioex.getMessage());
        }
        this.executor.shutdown();
    }
}
