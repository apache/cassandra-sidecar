package org.apache.cassandra.sidecar.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds file path after verifying it exists
 */
public abstract class FilePathBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(FilePathBuilder.class);
    private static final String dataSubDir = "/data";
    private final Collection<String> dataDirs;

    public FilePathBuilder(@NotNull final Collection<String> dataDirs)
    {
        this.dataDirs = dataDirs;
    }

    public abstract Path build(String keyspace, String table, String snapshot, String component)
            throws FileNotFoundException;

    String addKeyspaceToPath(String keyspace) throws FileNotFoundException
    {
        for (String dir : dataDirs)
        {
            StringBuilder path = new StringBuilder(dir);
            if (addFileToPathIfPresent(path, keyspace, true))
            {
                return path.toString();
            }

            if (dir.endsWith(dataSubDir))
            {
                continue;
            }

            // check in "data" sub directory
            if (addFileToPathIfPresent(path.append(dataSubDir), keyspace, true))
            {
                return path.toString();
            }
        }
        throw new FileNotFoundException("Keyspace " + keyspace + " does not exist");
    }

    String addTableToPath(String table, String path) throws FileNotFoundException
    {
        final StringBuilder modifiedPath = new StringBuilder(path);
        if (addFileToPathIfPresent(modifiedPath, table, false))
        {
            return modifiedPath.toString();
        }
        throw new FileNotFoundException("Table " + table + " not found, path searched: " + path);
    }

    String addSnapshotToPath(String snapshot, String path) throws FileNotFoundException
    {
        final StringBuilder modifiedPath = new StringBuilder(path);
        if (addFileToPathIfPresent(modifiedPath.append("/snapshots"), snapshot, true))
        {
            return modifiedPath.toString();
        }
        throw new FileNotFoundException("Snapshot " + snapshot + " not found, path searched: " + path);
    }

    String addSSTableComponentToPath(String component, String path) throws FileNotFoundException
    {
        final StringBuilder modifiedPath = new StringBuilder(path);
        if (addFileToPathIfPresent(modifiedPath, component, true))
        {
            return modifiedPath.toString();
        }
        throw new FileNotFoundException("Component " + component + " not found, path searched: " + path);
    }

    private boolean addFileToPathIfPresent(StringBuilder path, String file, boolean checkEqual)
            throws FileNotFoundException
    {
        final Path fileDir = Paths.get(path.toString());
        if (!checkDirExists(fileDir))
        {
            throw new FileNotFoundException(fileDir + " directory empty or does not exist!");
        }

        try
        {
            Path finalPath = null;
            try (final DirectoryStream<Path> dirEntries = Files.newDirectoryStream(fileDir))
            {
                for (Path entry : dirEntries)
                {
                    final Path filePath = entry.getFileName();
                    if (filePath == null)
                    {
                        continue;
                    }
                    final String fileName = filePath.toString();
                    if (fileName.equals(file) || (!checkEqual && fileName.startsWith(file + "-")))
                    {
                        if (finalPath == null
                                || Files.getLastModifiedTime(entry).compareTo(Files.getLastModifiedTime(finalPath)) > 0)
                        {
                            finalPath = entry;
                        }
                    }
                }
                if (finalPath != null)
                {
                    final Path finalFilePath = finalPath.getFileName();
                    if (finalFilePath == null)
                    {
                        return false;
                    }
                    final String finalFileName = finalFilePath.toString();
                    path.append('/').append(finalFileName);
                    return true;
                }
            }
        }
        catch (IOException e)
        {
            logger.error("Error listing files in path {}, could not add file {} to path", path, file, e);
            throw new RuntimeException("Failed to list files in path " + path);
        }
        return false;
    }

    private boolean checkDirExists(final Path path)
    {
        final File file = new File(path.toString());
        return file.exists() && file.isDirectory();
    }
}
