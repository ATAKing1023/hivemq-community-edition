/*
 * Copyright 2019-present HiveMQ GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hivemq.configuration.service.impl;

import com.hivemq.configuration.entity.ClusterEntity;
import com.hivemq.configuration.service.ClusterConfigurationService;
import com.hivemq.exceptions.UnrecoverableException;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;

/**
 * @author ankang
 */
@Slf4j
public class ClusterConfigurationServiceImpl implements ClusterConfigurationService {

    @NotNull
    private ClusterEntity clusterConfig = new ClusterEntity();

    @Override
    public ClusterEntity getClusterConfig() {
        return clusterConfig;
    }

    @Override
    public void setClusterConfig(final ClusterEntity clusterConfig) {
        final ClusterEntity.ClusterNode localNode = clusterConfig.getLocalNode();
        final List<ClusterEntity.ClusterNode> nodeList = clusterConfig.getNodeList();
        boolean found = false;
        for (final ClusterEntity.ClusterNode node : nodeList) {
            if (localNode.equals(node)) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw new IllegalArgumentException(
                    "Could not find local node " + localNode + " in node list: " + nodeList,
                    new UnrecoverableException());
        }
        if (nodeList.size() == 1) {
            int port = clusterConfig.getStartPort();
            while (!isPortAvailable(port)) {
                port++;
            }
            log.info("Detected standalone mode, using {} as start port", port);
            clusterConfig.setStartPort(port);
        }
        this.clusterConfig = clusterConfig;
    }

    private boolean isPortAvailable(final int port) {
        try (final ServerSocket serverSocket = new ServerSocket(port)) {
            return true;
        } catch (final IOException e) {
            log.info("Current port {} is not available", port);
            return false;
        }
    }
}
