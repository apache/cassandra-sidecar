package org.apache.cassandra.sidecar.common.data;

import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a data type for a Cassandra table column. Used by
 * {@link org.apache.cassandra.sidecar.routes.KeyspacesHandler} to serialize keyspace/table responses.
 */
public class DataType
{
    private final String name;
    private final boolean frozen;
    private final List<DataType> typeArguments;
    private final boolean collection;

    public DataType(@JsonProperty("name") String name,
                    @JsonProperty("frozen") boolean frozen,
                    @JsonProperty("typeArguments") List<DataType> typeArguments,
                    @JsonProperty("collection") boolean collection)
    {
        this.name = name;
        this.frozen = frozen;
        this.typeArguments = typeArguments;
        this.collection = collection;
    }

    /**
     * @return the name of that type.
     */
    public String getName()
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
        return new DataType(dataType.getName().name(),
                            dataType.isFrozen(),
                            typeArguments,
                            dataType.isCollection());
    }
}
