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

package com.hivemq.cluster.clientsession.rpc;

import com.hivemq.cluster.clientsession.ClientSessionSubscriptionOperation;
import com.hivemq.cluster.core.MqttClusterRequest;
import com.hivemq.cluster.core.MqttClusterRequestProcessor;
import com.hivemq.cluster.core.MqttClusterService;

/**
 * 客户端订阅新增请求处理类
 *
 * @author ankang
 * @since 2021/8/18
 */
public class ClientSessionSubscriptionAddRequestProcessor
        extends MqttClusterRequestProcessor<ClientSessionSubscriptionAddRequest> {

    public ClientSessionSubscriptionAddRequestProcessor(final MqttClusterService raftService) {
        super(raftService);
    }

    @Override
    protected MqttClusterRequest transform(final ClientSessionSubscriptionAddRequest request) {
        final ClientSessionSubscriptionOperation operation = new ClientSessionSubscriptionOperation();
        operation.setType(ClientSessionSubscriptionOperation.Type.ADD);
        operation.setClientId(request.getClientId());
        operation.setTopicObjects(request.getTopics());
        final MqttClusterRequest clusterRequest = new MqttClusterRequest();
        clusterRequest.setClientSessionSubscriptionOperation(operation);
        return clusterRequest;
    }

    @Override
    protected Class<ClientSessionSubscriptionAddRequest> getRequestClass() {
        return ClientSessionSubscriptionAddRequest.class;
    }
}
