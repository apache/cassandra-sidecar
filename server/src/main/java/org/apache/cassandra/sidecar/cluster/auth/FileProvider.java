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

package org.apache.cassandra.sidecar.cluster.auth;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;

import org.apache.cassandra.sidecar.exceptions.ConfigurationException;

/**
 * Loads CQL username/password from files.
 */
public class FileProvider implements CqlAuthProvider
{
    static final String USERNAME_PATH_PARAM = "username_path";
    static final String PASSWORD_PATH_PARAM = "password_path";

    private final Path usernamePath;
    private final Path passwordPath;

    public FileProvider(Map<String, String> parameters)
    {
        Objects.requireNonNull(parameters, "parameters must not be null");
        this.usernamePath = resolvePath(parameters, USERNAME_PATH_PARAM);
        this.passwordPath = resolvePath(parameters, PASSWORD_PATH_PARAM);
    }

    private static Path resolvePath(Map<String, String> parameters, String key)
    {
        if (!parameters.containsKey(key))
        {
            throw new ConfigurationException("Missing required auth_provider parameter \"" + key + "\"");
        }

        try
        {
            return Paths.get(parameters.get(key));
        }
        catch (InvalidPathException e)
        {
            throw new ConfigurationException("Invalid path in auth_provider parameter \"" + key + "\"", e);
        }
    }

    private static String readSecret(Path path, String key)
    {
        try
        {
            String secret = Files.readString(path).trim();
            if (secret.isEmpty())
            {
                throw new ConfigurationException("Empty content in auth_provider file for parameter \"" + key + "\"");
            }
            return secret;
        }
        catch (IOException e)
        {
            throw new ConfigurationException("Unable to read auth_provider file for parameter \"" + key + "\"", e);
        }
    }

  @Override
  public String username()
  {
        return readSecret(usernamePath, USERNAME_PATH_PARAM);
  }

  @Override
  public String password()
  {
        return readSecret(passwordPath, PASSWORD_PATH_PARAM);
  }
}
