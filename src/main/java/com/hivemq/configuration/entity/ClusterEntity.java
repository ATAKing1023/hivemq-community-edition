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

package com.hivemq.configuration.entity;

import com.hivemq.extension.sdk.api.annotations.NotNull;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * RheaKV集群配置
 *
 * @author ankang
 */
@XmlRootElement(name = "cluster")
@XmlAccessorType(XmlAccessType.NONE)
@SuppressWarnings({"FieldMayBeFinal", "FieldCanBeLocal"})
public class ClusterEntity {

    public static final int DEFAULT_START_PORT = 6000;

    /**
     * 节点名称
     */
    @XmlElement(name = "node-name", required = true)
    private @NotNull String nodeName = "HiveMQ";
    /**
     * 集群名称，部署多个集群时用于隔离，一般来说无需修改
     */
    @XmlElement(name = "cluster-name", required = true)
    private @NotNull String clusterName = "dev";
    /**
     * 监听地址
     */
    @XmlElement(name = "bind-address", required = true)
    private @NotNull String bindAddress = "127.0.0.1";
    /**
     * 存储服务监听的起始端口
     */
    @XmlElement(name = "start-port", required = true)
    private int startPort = DEFAULT_START_PORT;
    /**
     * 集群地址列表
     */
    @XmlElementWrapper(name = "node-list")
    @XmlElementRef(required = false)
    private @NotNull List<ClusterNode> nodeList = new ArrayList<>();

    public @NotNull String getNodeName() {
        return nodeName;
    }

    public @NotNull String getClusterName() {
        return clusterName;
    }

    public int getStartPort() {
        return startPort;
    }

    public void setStartPort(int startPort) {
        this.startPort = startPort;
    }

    public @NotNull String getBindAddress() {
        return bindAddress;
    }

    public @NotNull ClusterNode getLocalNode() {
        return new ClusterNode(getBindAddress(), getStartPort());
    }

    public @NotNull List<ClusterNode> getNodeList() {
        if (nodeList.isEmpty()) {
            synchronized (this) {
                if (nodeList.isEmpty()) {
                    nodeList.add(new ClusterNode(getBindAddress(), getStartPort()));
                }
            }
        }
        return nodeList;
    }

    @XmlRootElement(name = "node")
    @XmlAccessorType(XmlAccessType.NONE)
    public static class ClusterNode {

        @XmlElement(name = "host", required = true)
        private String host;
        @XmlElement(name = "start-port")
        private int startPort = DEFAULT_START_PORT;

        public ClusterNode() {
        }

        public ClusterNode(final String host, final int startPort) {
            this.host = host;
            this.startPort = startPort;
        }

        public String getHost() {
            return host;
        }

        public int getStartPort() {
            return startPort;
        }

        public String getAddress(final int portOffset) {
            return host + ":" + (startPort + portOffset);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ClusterNode)) {
                return false;
            }
            final ClusterNode that = (ClusterNode) o;
            return startPort == that.startPort && Objects.equals(host, that.host);
        }

        @Override
        public int hashCode() {
            return Objects.hash(host, startPort);
        }

        @Override
        public String toString() {
            return getAddress(0);
        }
    }
}
