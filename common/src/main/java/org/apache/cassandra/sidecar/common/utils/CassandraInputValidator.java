package org.apache.cassandra.sidecar.common.utils;

import java.io.File;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.ext.web.handler.HttpException;
import org.jetbrains.annotations.NotNull;

/**
 * Miscellaneous methods used for validation.
 */
@Singleton
public class CassandraInputValidator
{
    private final Set<String> forbiddenDirs;
    private final Pattern patternWordChars;
    private final String componentRegex;
    private final String dbTocComponentRegex;

    @Inject
    public CassandraInputValidator(ValidationConfiguration validationConfiguration)
    {
        forbiddenDirs = validationConfiguration.getForbiddenDirs();
        patternWordChars = Pattern.compile(validationConfiguration.getCharsAllowedPattern());
        componentRegex = validationConfiguration.getComponentRegex();
        dbTocComponentRegex = validationConfiguration.getDbTocComponentRegex();
    }

    public String validateKeyspaceName(final String keyspace)
    {
        Objects.requireNonNull(keyspace, "keyspace must not be null");
        validatePattern(keyspace, "keyspace");
        if (forbiddenDirs.contains(keyspace))
            throw new HttpException(HttpResponseStatus.FORBIDDEN.code(), "Forbidden keyspace: " + keyspace);
        return keyspace;
    }

    public String validateTableName(final String tableName)
    {
        Objects.requireNonNull(tableName, "tableName must not be null");
        validatePattern(tableName, "table name");
        return tableName;
    }

    public String validateSnapshotName(final String snapshotName)
    {
        Objects.requireNonNull(snapshotName, "snapshotName must not be null");
        //  most UNIX systems only disallow file separator and null characters for directory names
        if (snapshotName.contains(File.separator) || snapshotName.contains("\0"))
            throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(),
                                    "Invalid characters in snapshot name: " + snapshotName);
        return snapshotName;
    }

    public String validateComponentName(String componentName)
    {
        return validateComponentNameByRegex(componentName, componentRegex);
    }

    public String validateDbOrTOCComponentName(String componentName)
    {
        return validateComponentNameByRegex(componentName, dbTocComponentRegex);
    }

    @NotNull
    private String validateComponentNameByRegex(String componentName, String regex)
    {
        Objects.requireNonNull(componentName, "componentName must not be null");
        if (!componentName.matches(regex))
            throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(),
                                    "Invalid component name: " + componentName);
        return componentName;
    }

    private void validatePattern(String input, String name)
    {
        final Matcher matcher = patternWordChars.matcher(input);
        if (!matcher.matches())
            throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(),
                                    "Invalid characters in " + name + ": " + input);
    }
}
