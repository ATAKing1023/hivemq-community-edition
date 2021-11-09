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

package com.hivemq.cluster.clientqueue.rpc;

import com.hivemq.cluster.clientqueue.ClientQueueOperation;
import com.hivemq.cluster.core.AbstractMqttClusterRequestProcessor;
import com.hivemq.cluster.core.MqttClusterRequest;
import com.hivemq.cluster.core.MqttClusterService;

/**
 * 客户端队列消息发布请求处理类
 *
 * @author ankang
 * @since 2021/9/3
 */
public class ClientQueuePublishRequestProcessor extends AbstractMqttClusterRequestProcessor<ClientQueuePublishRequest> {

    public ClientQueuePublishRequestProcessor(final MqttClusterService raftService) {
        super(raftService);
    }

    @Override
    protected MqttClusterRequest transform(final ClientQueuePublishRequest request) {
        final ClientQueueOperation operation = new ClientQueueOperation();
        if (request.isShared()) {
            operation.setType(ClientQueueOperation.Type.SHARED_PUBLISH_AVAILABLE);
        } else {
            operation.setType(ClientQueueOperation.Type.PUBLISH_AVAILABLE);
        }
        operation.setQueueId(request.getQueueId());
        final MqttClusterRequest clusterRequest = new MqttClusterRequest();
        clusterRequest.setClientQueueOperation(operation);
        return clusterRequest;
    }

    @Override
    protected Class<ClientQueuePublishRequest> getRequestClass() {
        return ClientQueuePublishRequest.class;
    }
}
