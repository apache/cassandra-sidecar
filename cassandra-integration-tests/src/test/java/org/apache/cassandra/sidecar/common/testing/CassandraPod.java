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

package org.apache.cassandra.sidecar.common.testing;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.Exec;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;

import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodBuilder;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceBuilder;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;

import io.kubernetes.client.util.ClientBuilder;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.internal.Util;

/**
 * Manages a single instance of a Cassandra container
 */
class CassandraPod
{
    private final URI dockerRegistry;
    private final String image;
    private final String namespace;
    private final String dockerGroup;
    private static final Logger logger = LoggerFactory.getLogger(CassandraPod.class);
    private final String podName;

    private final CoreV1Api coreV1Api;
    private Boolean deleted = false;

    String ip;
    Integer port;

    public CassandraPod(URI dockerRegistry, String dockerGroup, String image, String namespace, CoreV1Api coreV1Api)
    {
        this.dockerRegistry = dockerRegistry;
        this.image = image;
        this.namespace = namespace;
        this.dockerGroup = dockerGroup;
        this.podName = String.format("cassandra-%s", UUID.randomUUID());
        this.coreV1Api = coreV1Api;
    }

    /**
     * Creates a single pod using the system properties passed in through Gradle
     * @param image
     * @return
     * @throws Exception
     */
    public static CassandraPod createFromProperties(String image) throws Exception
    {
        URI dockerRegistry = new URI(System.getProperty("sidecar.dockerRegistry"));
        String namespace = System.getProperty("sidecar.kubernetesNamespace");
        String dockerGroup = System.getProperty("sidecar.dockerGroup");

        logger.info("Creating pod from registry {}, namespace {}, group {}", dockerRegistry, namespace, dockerGroup);

        if (dockerRegistry == null)
        {
            throw new Exception("Docker registry required but sidecar.dockerRegistry = null");
        }
        if (namespace == null)
        {
            throw new Exception("sidecar.kubernetesNamespace is not defined and is required for K8 testing");
        }

        ApiClient apiClient = ClientBuilder.standard().build();

        // this is a workaround for socket errors that show up in certain JVM versions...
        // without it, the tests fail in CI.
        // we can probably get rid of this when we either move to JDK 11 only or if the Kubernetes clienti s updated
        OkHttpClient httpClient =
                apiClient.getHttpClient().newBuilder()
                        .protocols(Util.immutableList(Protocol.HTTP_1_1))
                        .readTimeout(10, TimeUnit.SECONDS)
                        .writeTimeout(10, TimeUnit.SECONDS)
                        .connectTimeout(10, TimeUnit.SECONDS)
                        .callTimeout(10, TimeUnit.SECONDS)
                        .retryOnConnectionFailure(true)
                        .build();
        apiClient.setHttpClient(httpClient);

        Configuration.setDefaultApiClient(apiClient);

        logger.info("K8 client: {}", apiClient.getBasePath());

        CoreV1Api coreV1Api = new CoreV1Api(apiClient);


        return new CassandraPod(dockerRegistry, dockerGroup, image, namespace, coreV1Api);
    }

    public void start() throws ApiException, InterruptedException, CassandraPodException
    {
        // create a v1 deployment spec
        String fullImage = getFullImageName();

        // similar to the spec yaml file, just programmatic

        HashMap<String, String> labels = getLabels();

        V1Service serviceBuilder = getService();

        logger.debug("Exposing service with: {}", serviceBuilder.toString());

        try
        {
            coreV1Api.createNamespacedService(namespace, serviceBuilder, null, null, null);
        }
        catch (ApiException e)
        {
            logger.error("Unable to create namespaced service: {}", e.getMessage());
            throw e;
        }

        // get the service
        V1Service namespacedService = coreV1Api.readNamespacedService(podName, namespace, null, null, null);
        logger.debug("Service result: {}", namespacedService);

        V1ServiceSpec serviceSpec = namespacedService.getSpec();

        logger.info("Starting container {}", fullImage);
        V1Pod pod = getPod(fullImage, labels);

        logger.debug("Pod spec: {}", pod);
        V1Pod podResult = coreV1Api.createNamespacedPod(namespace, pod, null, null, null);
        logger.debug("Pod result: {}", podResult);

        int maxTime = 120;
        V1Pod namespacedPod = null;
        Boolean started = false;
        String response = "";
        int sleepTimeInMs = 1000;

        for (int i = 0; i < maxTime; i++)
        {
            // we sleep in the beginning because the pod will never be ready right away
            // sometimes K8 seems to hang in CI as well, so this might be enough to let the pod start
            logger.debug("Reading namespace pod - sleeping for {}ms, ID: {}", sleepTimeInMs, podName);
            try
            {
                Thread.sleep(sleepTimeInMs);
            }
            catch (InterruptedException e)
            {
                logger.error("Unable to sleep: {}", e.getMessage());
                throw e;
            }
            namespacedPod = coreV1Api.readNamespacedPod(podName, namespace, null, null, null);
            response = namespacedPod.getStatus().getPhase();
            // not ready

            if (!response.contentEquals("Running"))
            {
                continue;
            }

            started = namespacedPod.getStatus().getContainerStatuses().get(0).getStarted();
            if (namespacedPod.getStatus().getContainerStatuses().get(0).getReady() && started)
            {
                logger.info("Pod startup OK");
                break;
            }

            logger.info("Container not ready: {}", response);
            Thread.sleep(1000);
        }
        if (!started)
        {
            throw new CassandraPodException("container not ready: " + response);
        }
        logger.debug("pod status: {}", namespacedPod);


        ip = serviceSpec.getClusterIP();

        List<V1ServicePort> ports = serviceSpec.getPorts();
        port = ports.get(0).getPort();
        logger.info("Cassandra pod {} running on {}:{}", podName, ip, port);
    }

    private HashMap<String, String> getLabels()
    {
        HashMap<String, String> labels = new HashMap<>();
        labels.put("name", podName);
        labels.put("purpose", "cassandra_sidecar_testing");
        return labels;
    }


    private V1Service getService()
    {
        return new V1ServiceBuilder()
                .withApiVersion("v1")
                .withKind("Service")
                .withNewMetadata()
                .withName(podName)
                .addToLabels("purpose", "cassandra_sidecar_testing")
                .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                .withType("NodePort")
                .addToPorts(new V1ServicePort().port(9042).protocol("TCP"))
                .addToSelector("name", podName)
                .endSpec()
                .build();
    }

    private V1Pod getPod(String fullImage, HashMap<String, String> labels)
    {
        return new V1PodBuilder()
                .withApiVersion("v1")
                .withKind("Pod")
                .withNewMetadata()
                    .withName(podName)
                    .withNamespace(namespace)
                    .withLabels(labels)
                    .endMetadata()
                .withNewSpec()
                    .addNewContainer()
                        .withName(podName)
                        .withImage(fullImage)
                        .addToPorts(new V1ContainerPort().containerPort(9042))
                        .withNewStartupProbe()
                            .withNewTcpSocket()
                                .withNewPort(9042)
                                .endTcpSocket()
                            .withInitialDelaySeconds(5)
                            .withPeriodSeconds(3)
                            .withFailureThreshold(30)
                            .endStartupProbe()
                        .endContainer()
                    .endSpec()
                .build();
    }

    public String getFullImageName()
    {
        return String.format("%s:%d/%s/%s", dockerRegistry.getHost(), dockerRegistry.getPort(), dockerGroup, image);
    }

    public void disableBinary() throws InterruptedException, ApiException, IOException
    {
        nodetool(new String[] {"disablebinary"});
    }

    public void enableBinary() throws InterruptedException, ApiException, IOException
    {
        nodetool(new String[] {"enablebinary"});
        // temporary sleep to ensure we start back up
        Thread.sleep(5000);
    }

    /**
     *
     */
    public void nodetool(String[] args) throws IOException, ApiException, InterruptedException
    {
        Exec exec = new Exec();
        List<String> command = new ArrayList<>();
        command.add("/cassandra/bin/nodetool");
        Collections.addAll(command, args);
        logger.info("Executing in container {}", command);

        Process proc = exec.exec(namespace, podName, command.toArray(new String[0]), false, false);
        proc.waitFor();
        proc.destroy();
    }


    /**
     * this is done asynchronously, so technically we could connect for a small window after the pod is deleted
     * not recommended to use mid-test
     */
    public void delete()
    {
        if (deleted)
        {
            logger.info("Pod already deleted, skipping");
            return;
        }
        deleteService();
        deletePod();
        deleted = true;

    }
    public String getIp()
    {
        return ip;
    }

    public Integer getPort()
    {
        return port;
    }

    private void deleteService()
    {
        try
        {
            logger.info("Deleting service {}", podName);
            coreV1Api.deleteNamespacedService(podName, namespace, null, null, null, null, null, null);

        }
        catch (Exception ex)
        {
            logger.info(String.format("Could not delete service %s", podName), ex);
        }
    }

    /**
     * Tries to delete a pod.  Might fail.
     * There's a variety of cases that can result in a pod not being deleted properly
     */
    private void deletePod()
    {
        try
        {
            logger.info("Deleting pod {}", podName);
            coreV1Api.deleteNamespacedPod(podName, namespace, null, null, null, null, null, null);
        }
        catch (Exception ex)
        {
            logger.error(String.format("Unable to delete pod %s", podName), ex);
        }
    }
}
