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

import com.alipay.sofa.jraft.Closure;
import com.hivemq.cluster.AbstractProcessor;
import com.hivemq.cluster.clientsession.ClientSessionOperation;
import com.hivemq.cluster.core.MqttClusterClosure;
import com.hivemq.cluster.core.MqttClusterRequest;
import com.hivemq.cluster.core.MqttClusterResponse;
import com.hivemq.cluster.core.MqttClusterService;

/**
 * 客户端会话移除处理器
 *
 * @author ankang
 * @since 2021/8/16
 */
public class ClientSessionRemoveRequestProcessor extends
        AbstractProcessor<MqttClusterRequest, MqttClusterResponse, MqttClusterClosure, MqttClusterService, ClientSessionRemoveRequest> {

    public ClientSessionRemoveRequestProcessor(final MqttClusterService raftService) {
        super(raftService);
    }

    @Override
    protected MqttClusterRequest transform(final ClientSessionRemoveRequest request) {
        final ClientSessionOperation operation = new ClientSessionOperation();
        operation.setType(ClientSessionOperation.Type.REMOVE);
        operation.setClientId(request.getClientId());
        operation.setSendWill(request.isSendWill());
        operation.setSessionExpiryInterval(request.getSessionExpiryInterval());
        final MqttClusterRequest clusterRequest = new MqttClusterRequest();
        clusterRequest.setClientSessionOperation(operation);
        return clusterRequest;
    }

    @Override
    protected MqttClusterResponse createResponse() {
        return new MqttClusterResponse();
    }

    @Override
    protected MqttClusterClosure createClosure(
            final MqttClusterRequest request, final MqttClusterResponse response, final Closure done) {
        return new MqttClusterClosure(request, response, done);
    }

    @Override
    protected Class<ClientSessionRemoveRequest> getRequestClass() {
        return ClientSessionRemoveRequest.class;
    }
}
