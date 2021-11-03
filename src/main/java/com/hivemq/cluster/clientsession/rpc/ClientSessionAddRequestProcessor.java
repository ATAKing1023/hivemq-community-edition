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

import com.hivemq.cluster.clientsession.ClientSessionOperation;
import com.hivemq.cluster.core.AbstractMqttClusterRequestProcessor;
import com.hivemq.cluster.core.MqttClusterRequest;
import com.hivemq.cluster.core.MqttClusterService;

/**
 * 客户端会话新增处理器
 *
 * @author ankang
 * @since 2021/8/11
 */
public class ClientSessionAddRequestProcessor extends AbstractMqttClusterRequestProcessor<ClientSessionAddRequest> {

    public ClientSessionAddRequestProcessor(final MqttClusterService raftService) {
        super(raftService);
    }

    @Override
    protected MqttClusterRequest transform(final ClientSessionAddRequest request) {
        final ClientSessionOperation operation = new ClientSessionOperation();
        operation.setType(ClientSessionOperation.Type.ADD);
        operation.setClientId(request.getClientId());
        operation.setCleanStart(request.isCleanStart());
        operation.setSessionExpiryInterval(request.getSessionExpiryInterval());
        operation.setWillPublish(request.getWillPublish());
        operation.setQueueLimit(request.getQueueLimit());
        final MqttClusterRequest clusterRequest = new MqttClusterRequest();
        clusterRequest.setClientSessionOperation(operation);
        return clusterRequest;
    }

    @Override
    protected Class<ClientSessionAddRequest> getRequestClass() {
        return ClientSessionAddRequest.class;
    }
}
