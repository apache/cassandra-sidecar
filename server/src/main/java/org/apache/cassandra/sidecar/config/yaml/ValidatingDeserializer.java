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

package org.apache.cassandra.sidecar.config.yaml;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.std.DelegatingDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import org.apache.cassandra.sidecar.config.Validatable;

/**
 * Deserializer that validates the configuration after it is deserialized.
 * Validates only classes that implement {@link Validatable} interface.
 */
public class ValidatingDeserializer extends DelegatingDeserializer
{
    ValidatingDeserializer(JsonDeserializer<?> delegate)
    {
        super(delegate);
    }

    @Override
    protected JsonDeserializer<?> newDelegatingInstance(JsonDeserializer<?> delegate)
    {
        return new ValidatingDeserializer(delegate);
    }

    @Override
    public Object deserialize(JsonParser parser, DeserializationContext context) throws IOException
    {
        return validate(parser, super.deserialize(parser, context));
    }

    @Override
    public Object deserialize(JsonParser parser, DeserializationContext context, Object intoValue) throws IOException
    {
        return validate(parser, super.deserialize(parser, context, intoValue));
    }

    @Override
    public Object deserializeWithType(JsonParser parser,
                                      DeserializationContext context,
                                      TypeDeserializer typeDeserializer) throws IOException
    {
        return validate(parser, super.deserializeWithType(parser, context, typeDeserializer));
    }

    /**
     * Performs validation on the deserialized object.
     * If the object implements {@link Validatable} interface, it will be validated.
     */
    private Object validate(JsonParser parser, Object value) throws JsonMappingException
    {
        if (value instanceof Validatable)
        {
            try
            {
                ((Validatable) value).validate();
            }
            catch (RuntimeException exception)
            {
                String message = String.format("Validation failed for %s: %s",
                                               value.getClass().getSimpleName(),
                                               exception.getMessage());
                throw JsonMappingException.from(parser, message, exception);
            }
        }
        return value;
    }
}
