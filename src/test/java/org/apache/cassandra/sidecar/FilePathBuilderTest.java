package org.apache.cassandra.sidecar;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.utils.CachedFilePathBuilder;
import org.apache.cassandra.sidecar.utils.FilePathBuilder;
import org.assertj.core.api.Assertions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * FilePathBuilderTest
 */
public class FilePathBuilderTest
{
    private static final String expectedFilePath = "src/test/resources/instance1/data/TestKeyspace" +
            "/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshots/TestSnapshot" +
            "/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
    private static FilePathBuilder pathBuilder;

    @BeforeAll
    public static void setUp()
    {
        Injector injector = Guice.createInjector(Modules.override(new MainModule()).with(new TestModule()));
        pathBuilder = injector.getInstance(InstancesConfig.class).instances().get(0).pathBuilder();
    }

    @Test
    public void testRoute() throws IOException
    {
        final String keyspace = "TestKeyspace";
        final String table = "TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b";
        final String snapshot = "TestSnapshot";
        final String component = "TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        Path filePath = pathBuilder.build(keyspace, table, snapshot, component);
        assertEquals(expectedFilePath, filePath.toString());
    }

    @Test
    public void testKeyspaceNotFound()
    {
        final String keyspace = "random";
        final String table = "TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b";
        final String snapshot = "TestSnapshot";
        final String component = "TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        FileNotFoundException thrownException = assertThrows(FileNotFoundException.class, () ->
        {
            pathBuilder.build(keyspace, table, snapshot, component);
        });
        String msg = "Keyspace random does not exist";
        assertEquals(msg, thrownException.getMessage());
    }

    @Test
    public void testTableNotFound()
    {
        final String keyspace = "TestKeyspace";
        final String table = "random";
        final String snapshot = "TestSnapshot";
        final String component = "TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        FileNotFoundException thrownException = assertThrows(FileNotFoundException.class, () ->
        {
            pathBuilder.build(keyspace, table, snapshot, component);
        });
        String msg = "Table random not found, path searched: src/test/resources/instance1/data/TestKeyspace";
        assertEquals(msg, thrownException.getMessage());
    }

    @Test
    public void testSnapshotNotFound()
    {
        final String keyspace = "TestKeyspace";
        final String table = "TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b";
        final String snapshot = "random";
        final String component = "TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        FileNotFoundException thrownException = assertThrows(FileNotFoundException.class, () ->
        {
            pathBuilder.build(keyspace, table, snapshot, component);
        });
        String msg = "Snapshot random not found, path searched: src/test/resources/instance1/data/TestKeyspace" +
                     "/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b";
        assertEquals(msg, thrownException.getMessage());
    }

    @Test
    public void testPartialTableName() throws FileNotFoundException
    {
        final String keyspace = "TestKeyspace";
        final String table = "TestTable";
        final String snapshot = "TestSnapshot";
        final String component = "TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        Path filePath = pathBuilder.build(keyspace, table, snapshot, component);
        assertEquals(expectedFilePath, filePath.toString());
    }

    @Test
    public void testEmptyDataDir() throws IOException
    {
        String dataDir = new File("./").getCanonicalPath() + "/src/test/resources/instance";

        final String keyspace = "TestKeyspace";
        final String table = "TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b";
        final String snapshot = "TestSnapshot";
        final String component = "TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";

        FilePathBuilder pathBuilder = new CachedFilePathBuilder(Collections.singletonList(dataDir));
        FileNotFoundException thrownException = assertThrows(FileNotFoundException.class, () ->
        {
            pathBuilder.build(keyspace, table, snapshot, component);
        });
        String msg = "directory empty or does not exist!";
        Assertions.assertThat(thrownException.getMessage()).contains(msg);
    }
}
