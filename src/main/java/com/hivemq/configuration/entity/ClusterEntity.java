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
import com.hivemq.extension.sdk.api.annotations.Nullable;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

/**
 * RheaKV集群配置
 *
 * @author ankang
 */
@XmlRootElement(name = "cluster")
@XmlAccessorType(XmlAccessType.NONE)
@SuppressWarnings({"FieldMayBeFinal", "FieldCanBeLocal"})
public class ClusterEntity {
    /**
     * 集群名称，一般来说无需修改
     */
    @XmlElement(name = "name", required = true)
    private @Nullable String name = "HiveMQ";
    /**
     * 存储服务监听的起始端口
     */
    @XmlElement(name = "start-port", required = true)
    private int startPort = 6000;
    /**
     * 监听地址
     */
    @XmlElement(name = "bind-address", required = true)
    private @NotNull String bindAddress = "127.0.0.1";
    /**
     * 集群地址列表
     */
    @XmlElementWrapper(name = "node-list")
    @XmlElement(name = "address")
    private @NotNull List<String> nodeList = defaultNodeList();

    public @NotNull String getName() {
        return name;
    }

    public int getStartPort() {
        return startPort;
    }

    public @NotNull String getBindAddress() {
        return bindAddress;
    }

    public @NotNull List<String> getNodeList() {
        return nodeList;
    }

    private @NotNull List<String> defaultNodeList() {
        final List<String> list = new ArrayList<>();
        list.add(bindAddress);
        return list;
    }
}
