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

package org.apache.cassandra.sidecar.livemigration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.UnknownMigrationPrefixException;
import org.mockito.Mockito;

import static org.apache.cassandra.sidecar.handlers.livemigration.InstanceMetadataTestUtil.LIVE_MIGRATION_CDC_RAW_DIR_PATH;
import static org.apache.cassandra.sidecar.handlers.livemigration.InstanceMetadataTestUtil.LIVE_MIGRATION_COMMITLOG_DIR_PATH;
import static org.apache.cassandra.sidecar.handlers.livemigration.InstanceMetadataTestUtil.LIVE_MIGRATION_DATA_FILE_DIR_PATH;
import static org.apache.cassandra.sidecar.handlers.livemigration.InstanceMetadataTestUtil.LIVE_MIGRATION_HINTS_DIR_PATH;
import static org.apache.cassandra.sidecar.handlers.livemigration.InstanceMetadataTestUtil.LIVE_MIGRATION_LOCAL_SYSTEM_DATA_FILE_DIR_PATH;
import static org.apache.cassandra.sidecar.handlers.livemigration.InstanceMetadataTestUtil.LIVE_MIGRATION_SAVED_CACHES_DIR_PATH;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil.localPath;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil.resolveLexically;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.CDC_RAW_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.COMMITLOG_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.DATA_FILE_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.HINTS_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.LOCAL_SYSTEM_DATA_FILE_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.SAVED_CACHES_DIR_PLACEHOLDER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.when;

class LiveMigrationInstanceMetadataUtilTest
{
    private static final String DATA_DIR = "data";
    private static final String HINTS_DIR = "hints";
    private static final String COMMITLOG_DIR = "commitlog";
    private static final String SAVED_CACHES_DIR = "saved_caches";
    private static final String CDC_RAW_DIR = "cdc_raw";
    private static final String LOCAL_SYSTEM_DIR = "local_system_data";
    private static final String STAGING_DIR = "sstable-staging";

    private static final String FILE_NAME = "file1.db";

    @TempDir
    Path tempDir;

    @Test
    public void testDirsToCopy()
    {
        String cassandraHomeDir = tempDir.resolve("testDirsToCopy").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);
        List<String> dataDirs = new ArrayList<>(2);
        String dataDir2 = DATA_DIR + "2";
        dataDirs.add(cassandraHomeDir + "/" + DATA_DIR);
        dataDirs.add(cassandraHomeDir + "/" + dataDir2);
        when(instanceMetadata.dataDirs()).thenReturn(dataDirs);

        List<String> dirsToCopy = LiveMigrationInstanceMetadataUtil.dirsToCopy(instanceMetadata);

        assertThat(dirsToCopy.size()).isEqualTo(7);
        assertThat(dirsToCopy).contains(cassandraHomeDir + "/" + DATA_DIR)
                              .contains(cassandraHomeDir + "/" + dataDir2)
                              .contains(cassandraHomeDir + "/" + HINTS_DIR)
                              .contains(cassandraHomeDir + "/" + COMMITLOG_DIR)
                              .contains(cassandraHomeDir + "/" + SAVED_CACHES_DIR)
                              .contains(cassandraHomeDir + "/" + CDC_RAW_DIR)
                              .contains(cassandraHomeDir + "/" + LOCAL_SYSTEM_DIR);
    }

    @Test
    public void testDirsToCopyFewDirsNotConfigured()
    {
        String cassandraHomeDir = tempDir.resolve("testDirsToCopyFewDirsNotConfigured").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);
        when(instanceMetadata.cdcDir()).thenReturn(null);
        when(instanceMetadata.localSystemDataFileDir()).thenReturn(null);

        List<String> dirsToCopy = LiveMigrationInstanceMetadataUtil.dirsToCopy(instanceMetadata);

        assertThat(dirsToCopy).hasSize(4);

        assertThat(dirsToCopy).doesNotContain(cassandraHomeDir + "/" + CDC_RAW_DIR)
                              .doesNotContain(cassandraHomeDir + "/" + LOCAL_SYSTEM_DIR);
    }

    @Test
    public void testDirPathPrefixMap()
    {
        String cassandraHomeDir = tempDir.resolve("testDirPathPrefixMap").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);
        List<String> dataDirs = new ArrayList<>(2);
        String dataDir2 = DATA_DIR + "2";
        dataDirs.add(cassandraHomeDir + "/" + DATA_DIR);
        dataDirs.add(cassandraHomeDir + "/" + dataDir2);
        when(instanceMetadata.dataDirs()).thenReturn(dataDirs);

        Map<String, String> dirPathPrefixMap = LiveMigrationInstanceMetadataUtil.dirPathPrefixMap(instanceMetadata);

        List<String> dirsToCopy = LiveMigrationInstanceMetadataUtil.dirsToCopy(instanceMetadata);
        assertThat(dirPathPrefixMap).hasSize(dirsToCopy.size());
        assertThat(dirPathPrefixMap.keySet()).containsAll(dirsToCopy);
        assertThat(dirPathPrefixMap).contains(
        entry(cassandraHomeDir + "/" + DATA_DIR, LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0"),
        entry(cassandraHomeDir + "/" + dataDir2, LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/1"),
        entry(cassandraHomeDir + "/" + HINTS_DIR, LIVE_MIGRATION_HINTS_DIR_PATH + "/0"),
        entry(cassandraHomeDir + "/" + COMMITLOG_DIR, LIVE_MIGRATION_COMMITLOG_DIR_PATH + "/0"),
        entry(cassandraHomeDir + "/" + SAVED_CACHES_DIR, LIVE_MIGRATION_SAVED_CACHES_DIR_PATH + "/0"),
        entry(cassandraHomeDir + "/" + CDC_RAW_DIR, LIVE_MIGRATION_CDC_RAW_DIR_PATH + "/0"),
        entry(cassandraHomeDir + "/" + LOCAL_SYSTEM_DIR, LIVE_MIGRATION_LOCAL_SYSTEM_DATA_FILE_DIR_PATH + "/0")
        );
    }

    @Test
    public void testDirPathPrefixMapFewDirsNotConfigured()
    {
        String cassandraHomeDir = tempDir.resolve("testDirsToCopyFewDirsNotConfigured").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);
        when(instanceMetadata.cdcDir()).thenReturn(null);
        when(instanceMetadata.localSystemDataFileDir()).thenReturn(null);

        Map<String, String> dirPathPrefixMap = LiveMigrationInstanceMetadataUtil.dirPathPrefixMap(instanceMetadata);

        List<String> dirsToCopy = LiveMigrationInstanceMetadataUtil.dirsToCopy(instanceMetadata);
        assertThat(dirPathPrefixMap).hasSize(dirsToCopy.size());
        assertThat(dirPathPrefixMap.keySet()).containsAll(dirsToCopy);
        assertThat(dirPathPrefixMap).doesNotContainKey(cassandraHomeDir + "/" + CDC_RAW_DIR)
                                    .doesNotContainKey(cassandraHomeDir + "/" + LOCAL_SYSTEM_DIR);
    }

    @Test
    public void testDirPlaceholderMap()
    {
        String cassandraHomeDir = tempDir.resolve("testDirPlaceholderMap").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);
        List<String> dataDirs = new ArrayList<>(2);
        String dataDir2 = DATA_DIR + "2";
        dataDirs.add(cassandraHomeDir + "/" + DATA_DIR);
        dataDirs.add(cassandraHomeDir + "/" + dataDir2);
        when(instanceMetadata.dataDirs()).thenReturn(dataDirs);

        Map<String, Set<String>> dirPathPrefixMap = LiveMigrationInstanceMetadataUtil.dirPlaceHoldersMap(instanceMetadata);

        List<String> dirsToCopy = LiveMigrationInstanceMetadataUtil.dirsToCopy(instanceMetadata);
        assertThat(dirPathPrefixMap).hasSize(dirsToCopy.size());
        assertThat(dirPathPrefixMap.keySet()).containsAll(dirsToCopy);
        assertThat(dirPathPrefixMap).contains(

        entry(cassandraHomeDir + "/" + DATA_DIR, new HashSet<>()
        {{
            add(DATA_FILE_DIR_PLACEHOLDER);
            add(DATA_FILE_DIR_PLACEHOLDER + "_" + 0);
        }}),
        entry(cassandraHomeDir + "/" + dataDir2, new HashSet<>()
        {{
            add(DATA_FILE_DIR_PLACEHOLDER);
            add(DATA_FILE_DIR_PLACEHOLDER + "_" + 1);
        }}),
        entry(cassandraHomeDir + "/" + HINTS_DIR, Collections.singleton(HINTS_DIR_PLACEHOLDER)),
        entry(cassandraHomeDir + "/" + COMMITLOG_DIR, Collections.singleton(COMMITLOG_DIR_PLACEHOLDER)),
        entry(cassandraHomeDir + "/" + SAVED_CACHES_DIR, Collections.singleton(SAVED_CACHES_DIR_PLACEHOLDER)),
        entry(cassandraHomeDir + "/" + CDC_RAW_DIR, Collections.singleton(CDC_RAW_DIR_PLACEHOLDER)),
        entry(cassandraHomeDir + "/" + LOCAL_SYSTEM_DIR, Collections.singleton(LOCAL_SYSTEM_DATA_FILE_DIR_PLACEHOLDER))

        );
    }

    @Test
    public void testDirPlaceholderMapFewDirsNotConfigured()
    {
        String cassandraHomeDir = tempDir.resolve("testDirPlaceholderMapFewDirsNotConfigured").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);
        when(instanceMetadata.cdcDir()).thenReturn(null);
        when(instanceMetadata.localSystemDataFileDir()).thenReturn(null);

        Map<String, Set<String>> dirPathPrefixMap = LiveMigrationInstanceMetadataUtil.dirPlaceHoldersMap(instanceMetadata);

        List<String> dirsToCopy = LiveMigrationInstanceMetadataUtil.dirsToCopy(instanceMetadata);
        assertThat(dirPathPrefixMap).hasSize(dirsToCopy.size());
        assertThat(dirPathPrefixMap.keySet()).containsAll(dirsToCopy);

        assertThat(dirPathPrefixMap).doesNotContainKey(cassandraHomeDir + "/" + CDC_RAW_DIR)
                                    .doesNotContainKey(cassandraHomeDir + "/" + LOCAL_SYSTEM_DIR);
    }

    @Test
    public void testPlaceholderDirsMap()
    {
        String cassandraHomeDir = tempDir.resolve("testPlaceholderDirsMap").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);
        List<String> dataDirs = new ArrayList<>(2);
        String dataDir2 = DATA_DIR + "2";
        dataDirs.add(cassandraHomeDir + "/" + DATA_DIR);
        dataDirs.add(cassandraHomeDir + "/" + dataDir2);
        when(instanceMetadata.dataDirs()).thenReturn(dataDirs);

        Map<String, Set<String>> placeholderDirsMap = LiveMigrationInstanceMetadataUtil.placeholderDirsMap(instanceMetadata);

        Set<String> dirsToCopy = new HashSet<>(LiveMigrationInstanceMetadataUtil.dirsToCopy(instanceMetadata));
        Set<String> dirsInMap = new HashSet<>();
        placeholderDirsMap.forEach((k, v) -> dirsInMap.addAll(v));

        // Placeholders should have been defined for all dirs that can be copied
        assertThat(dirsInMap).isEqualTo(dirsToCopy);

        assertThat(placeholderDirsMap).contains(
        entry(HINTS_DIR_PLACEHOLDER, Collections.singleton(instanceMetadata.hintsDir())),
        entry(COMMITLOG_DIR_PLACEHOLDER, Collections.singleton(instanceMetadata.commitlogDir())),
        entry(SAVED_CACHES_DIR_PLACEHOLDER, Collections.singleton(instanceMetadata.savedCachesDir())),
        entry(CDC_RAW_DIR_PLACEHOLDER, Collections.singleton(instanceMetadata.cdcDir())),
        entry(LOCAL_SYSTEM_DATA_FILE_DIR_PLACEHOLDER, Collections.singleton(instanceMetadata.localSystemDataFileDir())),
        entry(DATA_FILE_DIR_PLACEHOLDER, new HashSet<>(instanceMetadata.dataDirs())),
        entry(DATA_FILE_DIR_PLACEHOLDER + "_" + 0, Collections.singleton(instanceMetadata.dataDirs().get(0))),
        entry(DATA_FILE_DIR_PLACEHOLDER + "_" + 1, Collections.singleton(instanceMetadata.dataDirs().get(1)))
        );
    }

    @Test
    public void testPlaceholderDirsMapFewDirsNotConfigured()
    {
        String cassandraHomeDir = tempDir.resolve("testPlaceholderDirsMapFewDirsNotConfigured").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);
        when(instanceMetadata.cdcDir()).thenReturn(null);
        when(instanceMetadata.localSystemDataFileDir()).thenReturn(null);

        Map<String, Set<String>> placeholderDirsMap = LiveMigrationInstanceMetadataUtil.placeholderDirsMap(instanceMetadata);

        Set<String> dirsToCopy = new HashSet<>(LiveMigrationInstanceMetadataUtil.dirsToCopy(instanceMetadata));
        Set<String> dirsInMap = new HashSet<>();
        placeholderDirsMap.forEach((k, v) -> dirsInMap.addAll(v));

        // Placeholders should have been defined for all dirs that can be copied
        assertThat(dirsInMap).isEqualTo(dirsToCopy);

        assertThat(placeholderDirsMap).doesNotContainKey(CDC_RAW_DIR_PLACEHOLDER)
                                      .doesNotContainKey(LOCAL_SYSTEM_DATA_FILE_DIR_PLACEHOLDER);
    }

    @Test
    public void testLocalPath()
    {
        String cassandraHomeDir = tempDir.resolve("testLocalPath").toString();
        List<String> dataDirs = List.of(cassandraHomeDir  + "/" + DATA_DIR + 1,
                                        cassandraHomeDir + "/" + DATA_DIR + 2);
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir, dataDirs);

        validateLocalPath(instanceMetadata.dataDirs().get(0) + "/" + FILE_NAME,
                          LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/" + FILE_NAME,
                          instanceMetadata);
        validateLocalPath(instanceMetadata.dataDirs().get(1) + "/" + FILE_NAME,
                          LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/1/" + FILE_NAME,
                          instanceMetadata);
        validateLocalPath(instanceMetadata.cdcDir() + "/" + FILE_NAME,
                          LIVE_MIGRATION_CDC_RAW_DIR_PATH + "/0/" + FILE_NAME,
                          instanceMetadata);
        validateLocalPath(instanceMetadata.commitlogDir() + "/" + FILE_NAME,
                          LIVE_MIGRATION_COMMITLOG_DIR_PATH + "/0/" + FILE_NAME,
                          instanceMetadata);
        validateLocalPath(instanceMetadata.hintsDir() + "/" + FILE_NAME,
                          LIVE_MIGRATION_HINTS_DIR_PATH + "/0/" + FILE_NAME,
                          instanceMetadata);
        validateLocalPath(instanceMetadata.savedCachesDir() + "/" + FILE_NAME,
                          LIVE_MIGRATION_SAVED_CACHES_DIR_PATH + "/0/" + FILE_NAME,
                          instanceMetadata);
        validateLocalPath(instanceMetadata.localSystemDataFileDir() + "/" + FILE_NAME,
                          LIVE_MIGRATION_LOCAL_SYSTEM_DATA_FILE_DIR_PATH + "/0/" + FILE_NAME,
                          instanceMetadata);
    }

    @Test
    public void testRelativeLocalPaths()
    {
        String cassandraHomeDir = tempDir.resolve("testLocalPath").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);

        validateIllegalLocalPath(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/../" + FILE_NAME,
                                 instanceMetadata);
        validateIllegalLocalPath(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/../../" + FILE_NAME,
                                 instanceMetadata);
        validateIllegalLocalPath(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/" + FILE_NAME + "/..",
                                 instanceMetadata);
        validateIllegalLocalPath(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/../../../../etc/passwd",
                                 instanceMetadata);
        validateIllegalLocalPath(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/../../../../etc/passwd",
                                 instanceMetadata);
        validateIllegalLocalPath(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/" + FILE_NAME + "/../../../../etc/passwd",
                                 instanceMetadata);
    }

    @Test
    public void testLocalPathDoesNotEscapeForEncodedTraversal()
    {
        // Vert.x's normalizedPath() does a single unreserved-char decode followed by dot-segment removal.
        // A double-encoded "%252e%252e" survives as the literal "%252e%252e" (because "%25" is reserved and
        // is never decoded), and an encoded slash "%2f" stays encoded so "..%2f..%2f" is a single literal
        // path element rather than parent-directory segments. In both cases the sequence reaches localPath()
        // as ordinary file-name text, so the resolved path must stay *inside* the data directory and never
        // escape it. This locks in that the reported double-encoding bypass cannot traverse out of the root.
        String cassandraHomeDir = tempDir.resolve("encodedTraversal").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);
        String dataDir = instanceMetadata.dataDirs().get(0);

        // Double-encoded ".." segments (what survives Vert.x normalization for %252e%252e input)
        // resolve as literal directory names inside the data dir, not as parent-directory hops.
        assertThat(localPath(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/%252e%252e/%252e%252e/etc/passwd",
                             instanceMetadata).toString())
        .isEqualTo(dataDir + "/%252e%252e/%252e%252e/etc/passwd");

        // Encoded-slash sequence (what survives normalization for %2e%2e%2f input) stays a single literal
        // path element, so it too resolves inside the data dir.
        assertThat(localPath(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/ks/..%2f..%2fetc/passwd",
                             instanceMetadata).toString())
        .isEqualTo(dataDir + "/ks/..%2f..%2fetc/passwd");
    }

    @Test
    public void testRealPathRejectsSymlinkEscape() throws IOException
    {
        // A symlink inside the data dir pointing outside the migration tree must be rejected by the
        // realpath-based containment check; the lexical localPath() check alone cannot detect this.
        String cassandraHomeDir = tempDir.resolve("symlinkEscape").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);

        Path outside = tempDir.resolve("outside-tree");
        Files.createDirectories(outside);
        Files.write(outside.resolve("secret.txt"), "secret".getBytes(StandardCharsets.UTF_8));

        Path dataDir = Paths.get(instanceMetadata.dataDirs().get(0));
        Files.createDirectories(dataDir);
        Files.createSymbolicLink(dataDir.resolve("escape"), outside);

        assertThatIllegalArgumentException()
        .isThrownBy(() -> resolveLexically(
                          LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/escape/secret.txt", instanceMetadata)
                          .verifyContainment());
    }

    @Test
    public void testRealPathReturnsLexicalForSymlinkWithinDir() throws IOException
    {
        // A symlink whose target is still inside the data dir is legitimate; verifyContainment
        // accepts it without throwing, and resolveLexically returns the lexical (URL-derived)
        // path so downstream consumers operate against the configured directory namespace.
        String cassandraHomeDir = tempDir.resolve("symlinkWithin").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);

        Path dataDir = Paths.get(instanceMetadata.dataDirs().get(0));
        Files.createDirectories(dataDir.resolve("ks"));
        Files.write(dataDir.resolve("ks/file.db"), "data".getBytes(StandardCharsets.UTF_8));
        Files.createSymbolicLink(dataDir.resolve("link-ks"), dataDir.resolve("ks"));

        LiveMigrationInstanceMetadataUtil.ResolvedPath resolved =
            resolveLexically(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/link-ks/file.db", instanceMetadata);
        resolved.verifyContainment();
        assertThat(resolved.resolvedPath()).isEqualTo(dataDir.resolve("link-ks/file.db"));
    }

    @Test
    public void testRealPathThrowsForNonExistentPath()
    {
        // verifyContainment does an explicit Files.exists check before toRealPath and throws
        // NoSuchFileException when the resolved file is missing, so callers can distinguish
        // "file not found" cleanly from "path escapes base directory" (IllegalArgumentException).
        String cassandraHomeDir = tempDir.resolve("nonexistent").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);

        assertThatExceptionOfType(NoSuchFileException.class)
        .isThrownBy(() -> resolveLexically(
                          LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/ks/foo.db", instanceMetadata)
                          .verifyContainment());
    }

    @Test
    public void testRealPathRejectsLexicalTraversal()
    {
        // resolveLexically rejects /../ patterns up-front, before any filesystem I/O runs — same
        // guard that localPath uses.
        String cassandraHomeDir = tempDir.resolve("lexicalEscape").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);

        assertThatIllegalArgumentException()
        .isThrownBy(() -> resolveLexically(
                          LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/../../etc/passwd", instanceMetadata));
    }

    @Test
    public void testResolveLexicallyThrowsForUnknownPrefix()
    {
        // A well-formed URL whose prefix matches no configured directory is reported via
        // UnknownMigrationPrefixException. It is a subtype of IllegalArgumentException (so destination-side
        // localPath callers keep treating it as a bad argument), while the source-side handler catches this
        // specific type to answer 404 - "no such resource" - instead of 400 for a malformed URL.
        String cassandraHomeDir = tempDir.resolve("unknownPrefix").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);
        when(instanceMetadata.cdcDir()).thenReturn(null);

        assertThatExceptionOfType(UnknownMigrationPrefixException.class)
        .isThrownBy(() -> resolveLexically(LIVE_MIGRATION_CDC_RAW_DIR_PATH + "/0/" + FILE_NAME, instanceMetadata))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testLocalPathNonExistingDirs()
    {
        String cassandraHomeDir = tempDir.resolve("testGetLocalPathNonExistingDirs").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);
        when(instanceMetadata.cdcDir()).thenReturn(null);
        when(instanceMetadata.localSystemDataFileDir()).thenReturn(null);

        assertThatIllegalArgumentException()
        .isThrownBy(() -> localPath(LIVE_MIGRATION_CDC_RAW_DIR_PATH + "/0/" + FILE_NAME,
                                    instanceMetadata));

        assertThatIllegalArgumentException()
        .isThrownBy(() -> localPath(LIVE_MIGRATION_LOCAL_SYSTEM_DATA_FILE_DIR_PATH + "/0/" + FILE_NAME,
                                    instanceMetadata));
    }

    @Test
    public void testLocalPathInvalidDownloadUrls()
    {

        String cassandraHomeDir = tempDir.resolve("testLocalPathInvalidDownloadUrls").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);

        Function<String, String> localPath = (url) -> localPath(url, instanceMetadata).toString();

        assertThatIllegalArgumentException()
        .isThrownBy(() -> localPath.apply(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/2/" + FILE_NAME));

        assertThatIllegalArgumentException()
        .isThrownBy(() -> localPath.apply(LIVE_MIGRATION_CDC_RAW_DIR_PATH + "/1/" + FILE_NAME));

        assertThatIllegalArgumentException()
        .isThrownBy(() -> localPath.apply(LIVE_MIGRATION_COMMITLOG_DIR_PATH + "/1/" + FILE_NAME));

        assertThatIllegalArgumentException()
        .isThrownBy(() -> localPath.apply(LIVE_MIGRATION_HINTS_DIR_PATH + "/1/" + FILE_NAME));

        assertThatIllegalArgumentException()
        .isThrownBy(() -> localPath.apply(LIVE_MIGRATION_SAVED_CACHES_DIR_PATH + "/1/" + FILE_NAME));
        assertThatIllegalArgumentException()
        .isThrownBy(() -> localPath.apply(LIVE_MIGRATION_LOCAL_SYSTEM_DATA_FILE_DIR_PATH + "/1/" + FILE_NAME));
    }

    void validateLocalPath(String expectedPath, String fileDownloadUrl, InstanceMetadata instanceMetadata)
    {
        assertThat(localPath(fileDownloadUrl, instanceMetadata).toString()).isEqualTo(expectedPath);
    }

    void validateIllegalLocalPath(String fileDownloadUrl, InstanceMetadata instanceMetadata)
    {
        assertThatIllegalArgumentException()
        .isThrownBy(() -> localPath(fileDownloadUrl, instanceMetadata));
    }

    InstanceMetadata getInstanceMetadata(String cassandraHomeDir)
    {
        List<String> dataDirs = List.of(cassandraHomeDir + "/" + DATA_DIR);
        return getInstanceMetadata(cassandraHomeDir, dataDirs);
    }

    InstanceMetadata getInstanceMetadata(String cassandraHomeDir, List<String> dataDirs)
    {
        InstanceMetadata instanceMetadata = Mockito.mock(InstanceMetadata.class);
        when(instanceMetadata.dataDirs()).thenReturn(dataDirs);
        when(instanceMetadata.cdcDir()).thenReturn(cassandraHomeDir + "/" + CDC_RAW_DIR);
        when(instanceMetadata.commitlogDir()).thenReturn(cassandraHomeDir + "/" + COMMITLOG_DIR);
        when(instanceMetadata.hintsDir()).thenReturn(cassandraHomeDir + "/" + HINTS_DIR);
        when(instanceMetadata.savedCachesDir()).thenReturn(cassandraHomeDir + "/" + SAVED_CACHES_DIR);
        when(instanceMetadata.localSystemDataFileDir()).thenReturn(cassandraHomeDir + "/" + LOCAL_SYSTEM_DIR);
        when(instanceMetadata.stagingDir()).thenReturn(cassandraHomeDir + "/" + STAGING_DIR);

        return instanceMetadata;
    }
}
