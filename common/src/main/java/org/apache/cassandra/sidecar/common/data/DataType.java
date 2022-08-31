package org.apache.cassandra.sidecar.common.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Represents a data type for a Cassandra table column. Used by
 * {@link org.apache.cassandra.sidecar.routes.KeyspacesHandler} to serialize keyspace/table responses.
 */
public class DataType
{
    @NotNull
    private final String name;
    private final boolean frozen;
    private final List<DataType> typeArguments;
    private final boolean collection;

    @Nullable
    private final String customTypeClassName;

    @VisibleForTesting
    public DataType(@NotNull String name)
    {
        this(name, false, new ArrayList<>(), false, null);
    }

    public DataType(@JsonProperty("name") @NotNull String name,
                    @JsonProperty("frozen") boolean frozen,
                    @JsonProperty("typeArguments") List<DataType> typeArguments,
                    @JsonProperty("collection") boolean collection,
                    @JsonProperty("customTypeClassName") @Nullable String customTypeClassName)
    {
        if (collection)
        {
            Objects.requireNonNull(typeArguments, "typeArguments is required for collection data types");
            if ("MAP".equals(name))
            {
                checkArgument(typeArguments.size() == 2, "MAP data type takes 2 typeArguments");
            }
            if ("LIST".equals(name) || "SET".equals(name))
            {
                checkArgument(typeArguments.size() == 1, "LIST and SET data types take 1 typeArgument");
            }
        }
        else
        {
            checkArgument(typeArguments.isEmpty(),
                          "typeArguments only need to be provided for collection data types");
        }
        this.name = Objects.requireNonNull(name, "the name of the DataType must be non-null");
        this.frozen = frozen;
        this.typeArguments = typeArguments;
        this.collection = collection;
        this.customTypeClassName = customTypeClassName;
    }

    /**
     * @return the name of that type.
     */
    public @NotNull String getName()
    {
        return name;
    }

    /**
     * Returns whether this data type is frozen.
     *
     * <p>This applies to User Defined Types, tuples and nested collections. Frozen types are
     * serialized as a single value in Cassandra's storage engine, whereas non-frozen types are stored
     * in a form that allows updates to individual subfields.
     *
     * @return whether this data type is frozen.
     */
    public boolean isFrozen()
    {
        return frozen;
    }

    /**
     * Returns the type arguments of this type.
     *
     * <p>Note that only the collection types (LIST, MAP, SET) have type arguments. For the other
     * types, this will return an empty list.
     *
     * <p>For the collection types:
     *
     * <ul>
     *   <li>For lists and sets, this method returns one argument, the type of the elements.
     *   <li>For maps, this method returns two arguments, the first one is the type of the map keys,
     *       the second one is the type of the map values.
     * </ul>
     *
     * @return an immutable list containing the type arguments of this type.
     */
    public List<DataType> getTypeArguments()
    {
        return typeArguments;
    }

    /**
     * Returns whether this data type represent a CQL collection type, that is, a list, set or map.
     *
     * @return whether this data type name represent the name of a collection type.
     */
    public boolean isCollection()
    {
        return collection;
    }

    /**
     * Returns the fully qualified name of the subtype of {@code
     * org.apache.cassandra.db.marshal.AbstractType} that represents this type server-side.
     *
     * @return the fully qualified name of the subtype of {@code
     * org.apache.cassandra.db.marshal.AbstractType} that represents this type server-side.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public @Nullable String getCustomTypeClassName()
    {
        return customTypeClassName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(name, frozen, typeArguments, collection, customTypeClassName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataType dataType = (DataType) o;
        return frozen == dataType.frozen
               && collection == dataType.collection
               && name.equals(dataType.name)
               && Objects.equals(typeArguments, dataType.typeArguments)
               && Objects.equals(customTypeClassName, dataType.customTypeClassName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "DataType{" +
               "name='" + name + '\'' +
               ", frozen=" + frozen +
               ", typeArguments=" + typeArguments +
               ", collection=" + collection +
               ", customTypeClassName='" + customTypeClassName + '\'' +
               '}';
    }

    /**
     * Builds a {@link DataType} built from the given {@link com.datastax.driver.core.DataType dataType}.
     *
     * @param dataType the object that describes the Cassandra column type
     * @return a {@link DataType} built from the given {@link com.datastax.driver.core.DataType dataType}
     */
    public static DataType of(com.datastax.driver.core.DataType dataType)
    {
        List<DataType> typeArguments = dataType.getTypeArguments().stream()
                                               .map(DataType::of)
                                               .collect(Collectors.toList());
        String customTypeClassName = null;
        if (dataType instanceof com.datastax.driver.core.DataType.CustomType)
        {
            customTypeClassName = ((com.datastax.driver.core.DataType.CustomType) dataType).getCustomTypeClassName();
        }
        return new DataType(dataType.getName().name(),
                            dataType.isFrozen(),
                            typeArguments,
                            dataType.isCollection(),
                            customTypeClassName);
    }
}
