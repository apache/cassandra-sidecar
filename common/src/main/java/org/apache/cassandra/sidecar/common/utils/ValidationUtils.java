package org.apache.cassandra.sidecar.common.utils;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.ext.web.handler.HttpException;
import org.jetbrains.annotations.NotNull;

/**
 * Miscellaneous methods used for validation.
 */
public class ValidationUtils
{
    private static final Set<String> FORBIDDEN_DIRS = new HashSet<>(Arrays.asList("system_schema",
                                                                                  "system_traces",
                                                                                  "system_distributed",
                                                                                  "system",
                                                                                  "system_auth",
                                                                                  "system_views",
                                                                                  "system_virtual_schema"));
    private static final String CHARS_ALLOWED_PATTERN = "[a-zA-Z0-9_-]+";
    private static final Pattern PATTERN_WORD_CHARS = Pattern.compile(CHARS_ALLOWED_PATTERN);
    private static final String REGEX_COMPONENT = CHARS_ALLOWED_PATTERN + "(.db|.cql|.json|.crc32|TOC.txt)";
    private static final String REGEX_DB_TOC_COMPONENT = CHARS_ALLOWED_PATTERN + "(.db|TOC.txt)";

    public static String validateKeyspaceName(final String keyspace)
    {
        Objects.requireNonNull(keyspace, "keyspace must not be null");
        validatePattern(keyspace, "keyspace");
        if (FORBIDDEN_DIRS.contains(keyspace))
            throw new HttpException(HttpResponseStatus.FORBIDDEN.code(), "Forbidden keyspace: " + keyspace);
        return keyspace;
    }

    public static String validateTableName(final String tableName)
    {
        Objects.requireNonNull(tableName, "tableName must not be null");
        validatePattern(tableName, "table name");
        return tableName;
    }

    public static String validateSnapshotName(final String snapshotName)
    {
        Objects.requireNonNull(snapshotName, "snapshotName must not be null");
        //  most UNIX systems only disallow file separator and null characters for directory names
        if (snapshotName.contains(File.separator) || snapshotName.contains("\0"))
            throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(),
                                    "Invalid characters in snapshot name: " + snapshotName);
        return snapshotName;
    }

    public static String validateComponentName(String componentName)
    {
        return validateComponentNameByRegex(componentName, REGEX_COMPONENT);
    }

    public static String validateDbOrTOCComponentName(String componentName)
    {
        return validateComponentNameByRegex(componentName, REGEX_DB_TOC_COMPONENT);
    }

    @NotNull
    private static String validateComponentNameByRegex(String componentName, String regex)
    {
        Objects.requireNonNull(componentName, "componentName must not be null");
        if (!componentName.matches(regex))
            throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(),
                                    "Invalid component name: " + componentName);
        return componentName;
    }

    private static void validatePattern(String input, String name)
    {
        final Matcher matcher = PATTERN_WORD_CHARS.matcher(input);
        if (!matcher.matches())
            throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(),
                                    "Invalid characters in " + name + ": " + input);
    }
}
