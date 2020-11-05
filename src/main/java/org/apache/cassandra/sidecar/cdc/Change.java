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

package org.apache.cassandra.sidecar.cdc;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.partitions.PartitionUpdate;
/*
    _______________________________________________________________________________
    |  Envelope   | Payload   | Payload    |           |                           |
    |  Version    |  Type     |   Version  |   Flags   |   Serialized Payload bytes|
    | (1 byte)    | (1 byte)  | (1 byte)   |  (1 byte) |    (variable length)      |
    |_____________|___________|____________|___________|___________________________|

Envelope Version
================
Version of the envelope, changes with structural changes to the envelope.

Payload Type
============
The type of the payload. Defined in the PayloadType enum. Example
types are PartitionUpdate, Mutation, and URI.

Payload Version
===============
Some payloads (e.g. PartitionUpdate) need a version for de-serializing.

Flags
=====
Any custom flags that depends on the use case. E.g. someone can use these to differentiate
between snapshot and change events.

Serialized Payload
=====================
Serialized payload. This is just a byte array.

*/

/**
 * Defines the envelop of a CDC event
 *
 * */
public class Change
{
    public static final byte CHANGE_EVENT = 0;
    public static final byte REFRESH_EVENT = 1;
    // Envelop version, update with envelop changes.
    public static final byte ENVELOPE_VERSION = 1;
    // Side of the header, envelopeVersion + payloadType + payloadVersion + flags
    public static final int HEADER_SIZE = 4;

    private byte envelopeVersion;

    private byte payloadType;

    private byte payloadVersion;

    private byte flags;

    private byte[] payload;

    private String partitionKey;


    public Change(PayloadType payloadType, int version, byte flags, PartitionUpdate partitionUpdate)
    {
        this.envelopeVersion = Change.ENVELOPE_VERSION;
        this.payloadType = payloadType.getValue();
        this.payloadVersion = (byte) version;
        assert ((int) this.payloadVersion) == version;
        this.flags = flags;
        this.payload = PartitionUpdate.toBytes(partitionUpdate, this.payloadVersion).array();
        this.partitionKey = partitionUpdate.metadata().partitionKeyType.getString(partitionUpdate.partitionKey()
                .getKey());
    }

    public Change(byte[] serializedChange)
    {
        ByteBuffer buff = ByteBuffer.wrap(serializedChange);
        this.envelopeVersion = buff.get(0);
        assert this.envelopeVersion == Change.ENVELOPE_VERSION;
        this.payloadType = buff.get(1);
        this.payloadVersion = buff.get(2);
        this.flags = buff.get(3);
        this.payload = new byte[serializedChange.length - Change.HEADER_SIZE];
        buff.position(Change.HEADER_SIZE);
        buff.get(this.payload, 0, this.payload.length);
    }

    public byte[] toBytes() throws Exception
    {
        // We don't need to serialize the partition key
        ByteBuffer dob = ByteBuffer.allocate(Change.HEADER_SIZE + this.payload.length);
        dob.put(this.envelopeVersion);
        dob.put(this.payloadType);
        dob.put(this.payloadVersion);
        dob.put(this.flags);
        dob.put(this.payload);
        return dob.array();
    }

    public PartitionUpdate getPartitionUpdateObject() throws Exception
    {
        if (this.payload == null || this.payloadType != PayloadType.PARTITION_UPDATE.getValue())
        {
            throw new Exception(String.format("Invalid payloadType (%d), expected (%d)", this.payloadType,
                    PayloadType.PARTITION_UPDATE.getValue()));
        }
        return PartitionUpdate.fromBytes(ByteBuffer.wrap(this.payload), (int) this.payloadVersion);
    }

    public String getPartitionKey()
    {
        return this.partitionKey;
    }

    public int getPayloadVersion()
    {
        return (int) this.payloadVersion;
    }
}
