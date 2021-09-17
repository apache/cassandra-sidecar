package org.apache.cassandra.sidecar;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.apache.cassandra.sidecar.utils.CachedFilePathBuilder;
import org.apache.cassandra.sidecar.utils.FilePathBuilder;

/**
 * FilePathBuilderTest
 */
public class FilePathBuilderTest
{
    private static final String expectedFilePath = "src/test/resources/data/TestKeyspace" +
            "/TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b/snapshots/TestSnapshot" +
            "/TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
    private static FilePathBuilder pathBuilder;

    @BeforeClass
    public static void setUp()
    {
        Injector injector = Guice.createInjector(Modules.override(new MainModule()).with(new TestModule()));
        pathBuilder = injector.getInstance(FilePathBuilder.class);
    }

    @Test
    public void testRoute() throws IOException
    {
        final String keyspace = "TestKeyspace";
        final String table = "TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b";
        final String snapshot = "TestSnapshot";
        final String component = "TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        Path filePath = pathBuilder.build(keyspace, table, snapshot, component);
        Assert.assertEquals(expectedFilePath, filePath.toString());
    }

    @Test(expected = FileNotFoundException.class)
    public void testKeyspaceNotFound() throws FileNotFoundException
    {
        final String keyspace = "random";
        final String table = "TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b";
        final String snapshot = "TestSnapshot";
        final String component = "TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        pathBuilder.build(keyspace, table, snapshot, component);
    }

    @Test(expected = FileNotFoundException.class)
    public void testTableNotFound() throws FileNotFoundException
    {
        final String keyspace = "TestKeyspace";
        final String table = "random";
        final String snapshot = "TestSnapshot";
        final String component = "TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        pathBuilder.build(keyspace, table, snapshot, component);
    }

    @Test(expected = FileNotFoundException.class)
    public void testSnapshotNotFound() throws FileNotFoundException
    {
        final String keyspace = "TestKeyspace";
        final String table = "TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b";
        final String snapshot = "random";
        final String component = "TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        pathBuilder.build(keyspace, table, snapshot, component);
    }

    @Test
    public void testPartialTableName() throws FileNotFoundException
    {
        final String keyspace = "TestKeyspace";
        final String table = "TestTable";
        final String snapshot = "TestSnapshot";
        final String component = "TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";
        Path filePath = pathBuilder.build(keyspace, table, snapshot, component);
        Assert.assertEquals(expectedFilePath, filePath.toString());
    }

    @Test(expected = FileNotFoundException.class)
    public void testEmptyDataDir() throws IOException
    {
        String dataDir = new File("./").getCanonicalPath() + "/src/test/resources/instance";

        final String keyspace = "TestKeyspace";
        final String table = "TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b";
        final String snapshot = "TestSnapshot";
        final String component = "TestKeyspace-TestTable-54ea95ce-bba2-4e0a-a9be-e428e5d7160b-Data.db";

        FilePathBuilder pathBuilder = new CachedFilePathBuilder(Collections.singletonList(dataDir));
        pathBuilder.build(keyspace, table, snapshot, component);
    }
}
