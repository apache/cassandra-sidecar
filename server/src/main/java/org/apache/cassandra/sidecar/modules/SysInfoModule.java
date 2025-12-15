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

package org.apache.cassandra.sidecar.modules;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.ProvidesIntoMap;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.response.data.DiskInfo;
import org.apache.cassandra.sidecar.handlers.sysinfo.DiskInfoHandler;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.VertxRoute;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

/**
 * Guice module that provides system information-related routes and handlers.
 * Configures endpoints for retrieving system metrics such as disk information.
 */
public class SysInfoModule extends AbstractModule
{

    @GET
    @Path(ApiEndpointsV1.SYSTEM_DISK_INFO_ROUTE)
    @Operation(summary = "Retrieves disks information",
               description = "Retrieves information about disks of the current system")
    @APIResponse(description = "Disk information reported successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.ARRAY, implementation = DiskInfo.class,
                 example = "[{\"totalSpace\":1000000000000,\"freeSpace\":500000000000,\"usableSpace\":450000000000,\"name\":\"data1\",\"mount\":\"/dev/sda1\",\"type\":\"ext4\"},{\"totalSpace\":2000000000000,\"freeSpace\":1500000000000,\"usableSpace\":1400000000000,\"name\":\"data2\",\"mount\":\"/dev/sdb1\",\"type\":\"xfs\"}]")))
    @APIResponse(description = "Unauthorized - requires SYSTEM permission",
                 responseCode = "401")
    @APIResponse(description = "Service unavailable - unable to fetch disk information",
                 responseCode = "503")
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.SystemDiskInfoRoute.class)
    VertxRoute getDiskInfoHandler(RouteBuilder.Factory factory,
                                  DiskInfoHandler diskInfoHandler)
    {
        return factory.builderForRoute()
                      .handler(diskInfoHandler)
                      .build();
    }
}
