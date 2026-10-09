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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import com.datastax.driver.core.DataType;
import org.apache.cassandra.spark.utils.TableIdentifier;
import org.jetbrains.annotations.NotNull;

/**
 * Utility class providing CDC (Change Data Capture) specific operations and file handling.
 * This class offers a comprehensive set of static utility methods for working with CDC files,
 * parsing CDC-related data structures, and extracting schema information for CDC-enabled tables.
 * The utilities are specifically designed to handle Cassandra commit log segments and their
 * associated index files in the CDC context.
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
    public static final List<String> TABLE_PROPERTY_OVERRIDE_ALLOWLIST = List.of("min_index_interval", "max_index_interval", "cdc");

    // Matches a top-level-split column-defs entry that is itself the "PRIMARY KEY (...)"
    // clause, rather than a column definition — e.g. "PRIMARY KEY ((a, b), c)" or
    // "PRIMARY KEY (a, b)". Captures everything inside the outermost parentheses.
    private static final Pattern PRIMARY_KEY_CLAUSE_PATTERN = Pattern.compile("PRIMARY\\s+KEY\\s*\\((.*)\\)", Pattern.CASE_INSENSITIVE);
    // Matches a trailing "PRIMARY KEY" on a column definition line, e.g. "id uuid PRIMARY KEY".
    private static final Pattern INLINE_PRIMARY_KEY_SUFFIX = Pattern.compile("\\s+PRIMARY\\s+KEY\\s*$", Pattern.CASE_INSENSITIVE);
    // Matches a trailing "STATIC" on a column definition line, e.g. "count counter STATIC".
    private static final Pattern STATIC_SUFFIX = Pattern.compile("\\s+STATIC\\s*$", Pattern.CASE_INSENSITIVE);

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

        String lastLine = lines.get(lines.size() - 1);
        boolean isCompleted = lastLine.equals(LOG_FILE_COMPLETE_INDICATOR);
        long latestPosition = isCompleted ? segmentFileLength : Long.parseLong(lastLine);
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
        Matcher matcher = SEGMENT_PATTERN.matcher(name);
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
     * Validate for the cdc (log or index) file name.see {@link #SEGMENT_PATTERN} for the format
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

    /**
     * Holds a table's create statement and whether CDC is enabled on it.
     */
    public static class TableSchema
    {
        public final String createStatement;
        public final boolean cdc;
        public final PartitionKeySignature partitionKeySignature;

        public TableSchema(String createStatement, boolean cdc, PartitionKeySignature partitionKeySignature)
        {
            this.createStatement = createStatement;
            this.cdc = cdc;
            this.partitionKeySignature = partitionKeySignature;
        }
    }

    /**
     * A structural signature for a table's partition key: the ordered list of normalized CQL
     * type strings declared for each partition-key column. Column names don't matter — only
     * whether two tables' partition keys could ever serialize to the same bytes in a batch.
     *
     * <p>An {@link #indeterminate} signature (couldn't be confidently parsed) matches every
     * other signature via {@link #structurallyMatches} — a fail-safe so an unparseable table is
     * always included rather than silently excluded.
     */
    public static final class PartitionKeySignature
    {
        private static final PartitionKeySignature INDETERMINATE = new PartitionKeySignature(null, true);

        /**
         * Native CQL types with a fixed, known serialized width, mapped to that width in bytes.
         * Two types at the same width (e.g. {@code bigint}/{@code timestamp}, both 8 bytes) can
         * be byte-identical for every value, so {@link #structurallyMatches} treats them as
         * matching. Types not listed here (blob, text, varint, collections, UDTs, ...) have no
         * such guarantee and fall back to exact type-name equality instead.
         */
        private static final Map<DataType.Name, Integer> FIXED_WIDTH_BYTES =
            new ImmutableMap.Builder<DataType.Name, Integer>()
                .put(DataType.Name.TINYINT, 1)
                .put(DataType.Name.BOOLEAN, 1)
                .put(DataType.Name.SMALLINT, 2)
                .put(DataType.Name.INT, 4)
                .put(DataType.Name.DATE, 4)
                .put(DataType.Name.FLOAT, 4)
                .put(DataType.Name.BIGINT, 8)
                .put(DataType.Name.TIMESTAMP, 8)
                .put(DataType.Name.TIME, 8)
                .put(DataType.Name.DOUBLE, 8)
                .put(DataType.Name.UUID, 16)
                .put(DataType.Name.TIMEUUID, 16)
                .build();

        public final List<String> columnTypes;
        public final boolean indeterminate;

        private PartitionKeySignature(List<String> columnTypes, boolean indeterminate)
        {
            this.columnTypes = columnTypes;
            this.indeterminate = indeterminate;
        }

        public static PartitionKeySignature of(List<String> columnTypes)
        {
            return new PartitionKeySignature(new ArrayList<>(columnTypes), false);
        }

        public static PartitionKeySignature indeterminate()
        {
            return INDETERMINATE;
        }

        /**
         * @param other the other table's partition-key signature
         * @return true if a table with this signature could be co-located with a table with
         * {@code other}'s signature in the same commit-log {@code Mutation} (same keyspace,
         * same partition key bytes). Always true if either signature is indeterminate.
         */
        public boolean structurallyMatches(PartitionKeySignature other)
        {
            if (this.indeterminate || other.indeterminate)
            {
                return true;
            }
            if (this.columnTypes.size() != other.columnTypes.size())
            {
                return false;
            }
            for (int i = 0; i < this.columnTypes.size(); i++)
            {
                if (!canShareBytes(this.columnTypes.get(i), other.columnTypes.get(i)))
                {
                    return false;
                }
            }
            return true;
        }

        private static boolean canShareBytes(String typeA, String typeB)
        {
            if (typeA.equals(typeB))
            {
                return true;
            }
            Integer width = fixedWidthBytes(typeA);
            return width != null && width.equals(fixedWidthBytes(typeB));
        }

        private static Integer fixedWidthBytes(String normalizedType)
        {
            try
            {
                return FIXED_WIDTH_BYTES.get(DataType.Name.valueOf(normalizedType.toUpperCase()));
            }
            catch (IllegalArgumentException e)
            {
                return null;
            }
        }

        @Override
        public String toString()
        {
            return indeterminate ? "PartitionKeySignature{indeterminate}" : "PartitionKeySignature" + columnTypes;
        }
    }

    /**
     * Extracts ALL tables from the full cluster schema with their correct CDC flag.
     * The CDC flag is derived directly from the CREATE TABLE statement (cdc = true/false).
     * This allows callers to build {@link org.apache.cassandra.spark.data.CqlTable} objects
     * with the correct {@link org.apache.cassandra.spark.data.CqlTable#cdc()} value without
     * any pre-filtering — ensuring the bridge's Schema.instance is complete enough to
     * deserialize any commit log mutation without UnknownTableException.
     *
     * <p>Each returned {@link TableSchema} also carries a {@link PartitionKeySignature},
     * extracted cheaply (regex only, no real CQL grammar parsing) in this same pass, so callers
     * can decide which non-CDC tables are actually at risk of being batched with a CDC-enabled
     * table without needing to run the expensive {@code CassandraBridge.buildSchema()} parse
     * step on every table just to learn its partition-key structure.
     *
     * @param schemaStr full cluster schema text.
     * @return map of keyspace/table identifier to {@link TableSchema} (create statement + cdc
     * flag + partition-key signature).
     */
    public static Map<TableIdentifier, TableSchema> extractAllTablesWithCdcFlag(@NotNull String schemaStr)
    {
        String cleaned = cleanCql(schemaStr);
        Pattern pattern = Pattern.compile("CREATE TABLE \"?(\\w+)\"?\\.\"?(\\w+)\"?[^;]*;");
        Matcher matcher = pattern.matcher(cleaned);
        Map<TableIdentifier, TableSchema> result = new HashMap<>();
        while (matcher.find())
        {
            String keyspace = matcher.group(1);
            String table = matcher.group(2);
            // Detect CDC flag from the raw full schema fragment BEFORE extractCleanedTableSchema
            // strips table properties — the cleaned create statement omits cdc = true/false.
            String rawFragment = matcher.group(0);
            boolean cdc = rawFragment.contains("cdc = true");
            String createStmt = extractCleanedTableSchema(cleaned, keyspace, table);
            PartitionKeySignature pkSignature = extractPartitionKeySignature(rawFragment);
            result.put(TableIdentifier.of(keyspace, table), new TableSchema(createStmt, cdc, pkSignature));
        }
        return result;
    }

    /**
     * Extracts the {@link PartitionKeySignature} for a single table from its raw
     * (whitespace-cleaned via {@link #cleanCql}, but not yet property-stripped) CREATE TABLE
     * fragment. Cheap and regex-based — never invokes real CQL grammar parsing — so it is safe
     * to run for every table in the schema.
     *
     * <p>Supports both CQL primary key declaration forms:
     * <ul>
     *   <li>Inline single-column: {@code col type PRIMARY KEY}</li>
     *   <li>Trailing clause, single or composite partition key: {@code PRIMARY KEY (col, ...)}
     *   or {@code PRIMARY KEY ((colA, colB), ...)}</li>
     * </ul>
     *
     * <p>UDT-typed partition-key columns are treated as opaque tokens (the UDT's name, not its
     * expanded field structure) — sufficient because a real batch statement requires the
     * literal same CQL type on both sides to bind matching partition key values, and UDT names
     * are unique within a keyspace.
     *
     * <p>Any parsing ambiguity (unrecognized syntax, unresolvable column type, missing primary
     * key) results in {@link PartitionKeySignature#indeterminate()} rather than a guess, so
     * callers never silently exclude a table they're unsure about.
     */
    private static PartitionKeySignature extractPartitionKeySignature(String tableFragment)
    {
        try
        {
            String withColumnDefs = removeTableProps(tableFragment);
            int firstParen = withColumnDefs.indexOf('(');
            if (firstParen < 0 || !withColumnDefs.endsWith(")"))
            {
                return PartitionKeySignature.indeterminate();
            }
            String columnDefsBlock = withColumnDefs.substring(firstParen + 1, withColumnDefs.length() - 1);

            Map<String, String> columnTypes = new HashMap<>();
            String inlinePkColumn = null;
            String primaryKeyClauseInner = null;

            for (String rawPart : splitTopLevel(columnDefsBlock, ','))
            {
                String part = rawPart.trim();
                if (part.isEmpty())
                {
                    continue;
                }

                Matcher pkClauseMatcher = PRIMARY_KEY_CLAUSE_PATTERN.matcher(part);
                if (pkClauseMatcher.matches())
                {
                    primaryKeyClauseInner = pkClauseMatcher.group(1).trim();
                    continue;
                }

                // column definition: "name type [STATIC] [PRIMARY KEY]"
                boolean isInlinePk = false;
                Matcher inlinePk = INLINE_PRIMARY_KEY_SUFFIX.matcher(part);
                if (inlinePk.find())
                {
                    isInlinePk = true;
                    part = part.substring(0, inlinePk.start());
                }
                Matcher staticMatcher = STATIC_SUFFIX.matcher(part);
                if (staticMatcher.find())
                {
                    part = part.substring(0, staticMatcher.start());
                }

                int firstSpace = findNameTypeSplitIndex(part);
                if (firstSpace < 0)
                {
                    // malformed column definition — can't determine name/type split
                    return PartitionKeySignature.indeterminate();
                }
                String columnName = stripQuotes(part.substring(0, firstSpace).trim());
                String type = normalizeType(part.substring(firstSpace + 1));
                columnTypes.put(columnName, type);

                if (isInlinePk)
                {
                    inlinePkColumn = columnName;
                }
            }

            List<String> partitionKeyColumnNames;
            if (primaryKeyClauseInner != null)
            {
                partitionKeyColumnNames = parsePartitionKeyColumnNames(primaryKeyClauseInner);
            }
            else if (inlinePkColumn != null)
            {
                partitionKeyColumnNames = List.of(inlinePkColumn);
            }
            else
            {
                // no PRIMARY KEY found at all — shouldn't happen for a valid schema
                return PartitionKeySignature.indeterminate();
            }

            List<String> signature = new ArrayList<>(partitionKeyColumnNames.size());
            for (String columnName : partitionKeyColumnNames)
            {
                String type = columnTypes.get(columnName);
                if (type == null)
                {
                    // partition key references a column we couldn't resolve a type for
                    return PartitionKeySignature.indeterminate();
                }
                signature.add(type);
            }
            return PartitionKeySignature.of(signature);
        }
        catch (RuntimeException e)
        {
            // never let a schema-parsing edge case break the whole refresh cycle — fail safe
            return PartitionKeySignature.indeterminate();
        }
    }

    /**
     * Parses the partition-key column names, in order, from the inner content of a
     * {@code PRIMARY KEY (...)} clause — e.g. {@code "(colA, colB), colC"} (composite partition
     * key) or {@code "colA, colB"} (single-column partition key, colA, with colB as a
     * clustering column) or {@code "colA"} (single-column partition key, no clustering).
     */
    private static List<String> parsePartitionKeyColumnNames(String primaryKeyClauseInner)
    {
        String inner = primaryKeyClauseInner.trim();

        List<String> topLevelParts = splitTopLevel(inner, ',');
        if (topLevelParts.isEmpty())
        {
            return List.of();
        }

        String first = topLevelParts.get(0).trim();
        if (first.startsWith("(") && first.endsWith(")"))
        {
            // composite partition key: (colA, colB), colC, ...  or  (colA, colB) with no
            // clustering columns at all
            String compositeInner = first.substring(1, first.length() - 1);
            List<String> names = new ArrayList<>();
            for (String part : splitTopLevel(compositeInner, ','))
            {
                names.add(stripQuotes(part.trim()));
            }
            return names;
        }
        else
        {
            // single-column partition key: colA, colB, colC (colA alone is the partition key)
            return List.of(stripQuotes(first));
        }
    }

    private static String stripQuotes(String identifier)
    {
        if (identifier.length() >= 2 && identifier.startsWith("\"") && identifier.endsWith("\""))
        {
            return identifier.substring(1, identifier.length() - 1);
        }
        return identifier;
    }

    /**
     * Finds the index of the whitespace separating a column definition's name from its type,
     * e.g. {@code id uuid} -&gt; 2. A plain {@code indexOf(' ')} is not sufficient when the
     * column name is a quoted identifier that itself contains a space (e.g.
     * {@code "my col" uuid}) — the first space found would be inside the quotes, splitting the
     * name/type incorrectly. In that case, skip past the closing quote first.
     *
     * @return the split index, or -1 if no valid separator could be found
     */
    private static int findNameTypeSplitIndex(String part)
    {
        if (part.startsWith("\""))
        {
            int closingQuote = part.indexOf('"', 1);
            if (closingQuote < 0)
            {
                // unterminated quoted identifier — malformed
                return -1;
            }
            return part.indexOf(' ', closingQuote + 1);
        }
        return part.indexOf(' ');
    }

    private static String normalizeType(String type)
    {
        return type.trim().toLowerCase().replaceAll("\\s+", "");
    }

    /**
     * Splits {@code s} on top-level occurrences of {@code delimiter}, ignoring any delimiter
     * nested inside {@code (...)} or {@code <...>} — so CQL constructs like
     * {@code frozen<map<int, text>>} or {@code PRIMARY KEY ((a, b), c)} split correctly.
     */
    private static List<String> splitTopLevel(String s, char delimiter)
    {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < s.length(); i++)
        {
            char c = s.charAt(i);
            if (c == '(' || c == '<')
            {
                depth++;
            }
            else if (c == ')' || c == '>')
            {
                depth--;
            }
            else if (c == delimiter && depth == 0)
            {
                parts.add(s.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(s.substring(start));
        return parts;
    }

    public static String cleanCql(@NotNull final String cql)
    {
        return cql.replaceAll("(\\\\r|\\\\n|\\\\r\\n)+", "\n")
                  .replaceAll("\n", "")
                  .replaceAll("\\\\", "");
    }

    public static String extractCleanedTableSchema(@NotNull String cleaned,
                                                   @NotNull String keyspace,
                                                   @NotNull String table)
    {
        Pattern pattern = Pattern.compile(String.format("CREATE TABLE ?\"?%s?\"?\\.{1}\"?%s\"?[^;]*;", keyspace, table));
        Matcher matcher = pattern.matcher(cleaned);
        if (matcher.find())
        {
            String fullSchema = cleaned.substring(matcher.start(0), matcher.end(0));
            String tableOnly = removeTableProps(fullSchema);
            String quotedTableName = String.format("\"%s\"", table);
            if (tableOnly.contains(quotedTableName))
            {
                // remove quoted table name from schema
                tableOnly = tableOnly.replaceFirst(quotedTableName, table);
            }
            String redactedSchema = tableOnly;
            String clustering = extractClustering(fullSchema);
            String separator = " WITH ";
            if (clustering != null)
            {
                redactedSchema = redactedSchema + separator + clustering;
                separator = " AND ";
            }

            List<String> propStrings = extractOverrideProperties(fullSchema, TABLE_PROPERTY_OVERRIDE_ALLOWLIST);
            if (!propStrings.isEmpty())
            {
                redactedSchema = redactedSchema + separator + String.join(" AND ", propStrings);
                separator = " AND "; // for completeness
            }
            return redactedSchema + ";";
        }
        throw new RuntimeException(String.format("Could not find schema for table: %s.%s", keyspace, table));
    }

    private static String removeTableProps(@NotNull String schema)
    {
        int pos = schema.indexOf('(');
        int count = 1;
        if (pos < 0)
        {
            throw new RuntimeException("Missing parentheses in table schema " + schema);
        }
        while (++pos < schema.length()) // find closing bracket
        {
            if (schema.charAt(pos) == ')')
            {
                count--;
            }
            else if (schema.charAt(pos) == '(')
            {
                count++;
            }
            if (count == 0)
            {
                break;
            }
        }
        return schema.substring(0, pos + 1);
    }

    @VisibleForTesting
    static String extractClustering(String schemaStr)
    {
        Pattern pattern = Pattern.compile("CLUSTERING ORDER BY \\([^)]*");
        Matcher matcher = pattern.matcher(schemaStr);
        if (matcher.find())
        {
            return schemaStr.substring(matcher.start(0), matcher.end(0) + 1);
        }
        return null;
    }

    static List<String> extractOverrideProperties(String schemaStr, List<String> properties)
    {
        List<String> overrideTableProps = new ArrayList<>();
        if (properties.isEmpty()) return overrideTableProps;
        Pattern pattern = Pattern.compile("(" + properties.stream().collect(Collectors.joining("|")) + ") = (\\w+)");
        Matcher matcher = pattern.matcher(schemaStr);

        while (matcher.find())
        {
            String parsedProp = schemaStr.substring(matcher.start(), matcher.end());
            overrideTableProps.add(parsedProp);
        }
        return overrideTableProps;
    }
}
