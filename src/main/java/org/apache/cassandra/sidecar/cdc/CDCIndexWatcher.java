package org.apache.cassandra.sidecar.cdc;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.sun.nio.file.SensitivityWatchEventModifier;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.Pair;
/**
 * Watches CDC index and produce commit log offsets to be read and processed.
 */
@Singleton
public class CDCIndexWatcher implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(CDCIndexWatcher.class);
    private WatchService watcher;
    private WatchKey key;
    private Path dir;
    private final org.apache.cassandra.sidecar.cdc.CommitLogReader commitLogReader;
    private ExecutorService cdcWatcherExecutor;
    private volatile boolean running;

    @Inject
    CDCIndexWatcher(CommitLogReader commitLogReader)
    {
        this.commitLogReader = commitLogReader;
        this.running = true;
    }

    @Override
    public void run()
    {
        this.dir = Paths.get(DatabaseDescriptor.getCDCLogLocation());
        this.cdcWatcherExecutor = Executors.newSingleThreadExecutor();

        this.cdcWatcherExecutor.submit(() ->
        {
            try
            {
                this.watcher = FileSystems.getDefault().newWatchService();
                this.key = Paths.get(dir.toUri()).register(this.watcher,
                        new WatchEvent.Kind[]{ENTRY_MODIFY},
                        SensitivityWatchEventModifier.HIGH);
                while (running)
                {
                    WatchKey aKey = watcher.take();
                    if (!key.equals(aKey))
                    {
                        logger.error("WatchKey not recognized.");
                        continue;
                    }
                    for (WatchEvent<?> event : key.pollEvents())
                    {
                        WatchEvent.Kind<?> kind = event.kind();
                        WatchEvent<Path> ev = (WatchEvent<Path>) event;
                        Path relativePath = ev.context();
                        Path absolutePath = dir.resolve(relativePath);
                        logger.debug("Event type : {}, Path : {}", event.kind().name(), absolutePath);
                        logger.debug("Event timestamp in milliseconds : {}", Instant.now().toEpochMilli());
                        if (!CommitLogReader.isValidIndexFile(absolutePath.getFileName().toString()))
                        {
                            continue;
                        }
                        this.commitLogReader.submitReadRequest(Pair.create(absolutePath, 0));
                    }
                    key.reset();
                }
            }
            catch (Throwable throwable)
            {
                logger.error("Error when watching the CDC dir : {}", throwable.getMessage());
            }
        });
        this.commitLogReader.start();
    }

    public void stop()
    {
        running = false;
        this.commitLogReader.stop();
        this.cdcWatcherExecutor.shutdown();
    }
}
