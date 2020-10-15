package org.apache.cassandra.sidecar.cdc;

/**
 * Enum representing the payload payloadType of a Change
 */
public enum PayloadType
{
    PARTITION_UPDATE((byte) 0),
    MUTATION((byte) 1),
    URI((byte) 2);

    private byte value;

    PayloadType(byte value)
    {
        this.value = value;
    }

    public byte getValue()
    {
        return this.value;
    }
}
