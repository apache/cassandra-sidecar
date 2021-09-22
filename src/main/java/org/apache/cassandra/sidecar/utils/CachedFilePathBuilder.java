package org.apache.cassandra.sidecar.utils;

import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyException;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Path builder that caches intermediate paths
 */
@Singleton
public class CachedFilePathBuilder extends FilePathBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(CachedFilePathBuilder.class);
    private final CacheLoader<Key, String> loader = new PathCacheLoader();
    private final LoadingCache<Key, String> sstableCache = getCacheBuilder();
    private final LoadingCache<Key, String> snapshotCache = getCacheBuilder();
    private final LoadingCache<Key, String> tableCache = getCacheBuilder();
    private final LoadingCache<Key, String> keyspaceCache = getCacheBuilder();

    private LoadingCache<Key, String> getCacheBuilder()
    {
        return CacheBuilder.newBuilder().maximumSize(10000).refreshAfterWrite(5, TimeUnit.MINUTES).build(loader);
    }

    @Inject
    public CachedFilePathBuilder(final Collection<String> dataDirs)
    {
        super(dataDirs);
    }

    public Path build(String keyspace, String table, String snapshot, String component) throws FileNotFoundException
    {
        try
        {
            return Paths.get(sstableCache.get(new Key.Builder().setKeyspace(keyspace).setTable(table)
                    .setSnapshot(snapshot).setComponent(component).build()));
        }
        catch (Throwable t)
        {
            if (ExceptionUtils.getRootCause(t) instanceof FileNotFoundException)
            {
                throw (FileNotFoundException) ExceptionUtils.getRootCause(t);
            }
            else
            {
                logger.error("Unexpected error while building path ", t);
                throw new RuntimeException("Error loading value from path cache");
            }
        }
    }

    /**
     * Cache Loader for guava cache storing path to files
     */
    public class PathCacheLoader extends CacheLoader<Key, String>
    {
        @Override
        public String load(Key key) throws FileNotFoundException, KeyException, ExecutionException
        {
            switch (key.type())
            {
                case KEYSPACE_TABLE_SNAPSHOT_COMPONENT:
                    return addSSTableComponentToPath(key.component(), snapshotCache.get(new Key.Builder()
                            .setKeyspace(key.keyspace()).setTable(key.table()).setSnapshot(key.snapshot()).build()));
                case KEYSPACE_TABLE_SNAPSHOT:
                    return addSnapshotToPath(key.snapshot(), tableCache.get(new Key.Builder()
                            .setKeyspace(key.keyspace()).setTable(key.table()).build()));
                case KEYSPACE_TABLE:
                    return addTableToPath(key.table(), keyspaceCache.get(new Key.Builder().setKeyspace(key.keyspace())
                            .build()));
                case JUST_KEYSPACE:
                    return addKeyspaceToPath(key.keyspace());
                default:
                    throw new KeyException();
            }
        }
    }

    /**
     * Key to retrieve path information from cache
     */
    public static class Key
    {
        private final String keyspace;
        private final String table;
        private final String snapshot;
        private final String component;
        private final KeyType type;

        private Key(String keyspace, String table, String snapshot, String component, KeyType type)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.snapshot = snapshot;
            this.component = component;
            this.type = type;
        }

        public String keyspace() throws KeyException
        {
            return Optional.ofNullable(keyspace).orElseThrow(KeyException::new);
        }

        public String table() throws KeyException
        {
            return Optional.ofNullable(table).orElseThrow(KeyException::new);
        }

        public String snapshot() throws KeyException
        {
            return Optional.ofNullable(snapshot).orElseThrow(KeyException::new);
        }

        public String component() throws KeyException
        {
            return Optional.ofNullable(component).orElseThrow(KeyException::new);
        }

        public KeyType type()
        {
            return type;
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
            Key key = (Key) o;
            return type == key.type &&
                   Objects.equals(keyspace, key.keyspace) &&
                   Objects.equals(table, key.table) &&
                   Objects.equals(snapshot, key.snapshot) &&
                   Objects.equals(component, key.component);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(keyspace, table, snapshot, component, type);
        }

        /**
         * Builder class for Key
         */
        public static class Builder
        {
            private String keyspace;
            private String table;
            private String snapshot;
            private String component;
            private int length;

            public Builder setKeyspace(String keyspace)
            {
                length++;
                this.keyspace = keyspace;
                return this;
            }

            public Builder setTable(String table)
            {
                length++;
                this.table = table;
                return this;
            }

            public Builder setSnapshot(String snapshot)
            {
                length++;
                this.snapshot = snapshot;
                return this;
            }

            public Builder setComponent(String component)
            {
                length++;
                this.component = component;
                return this;
            }

            public CachedFilePathBuilder.Key build()
            {
                return new CachedFilePathBuilder.Key(keyspace, table, snapshot, component,
                        KeyType.values()[length - 1]);
            }
        }
    }

    /**
     * Enum to hold types of keys created
     */
    public enum KeyType
    {
        JUST_KEYSPACE, KEYSPACE_TABLE, KEYSPACE_TABLE_SNAPSHOT, KEYSPACE_TABLE_SNAPSHOT_COMPONENT
    }
}
