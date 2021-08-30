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
import com.hivemq.cluster.clientsession.ClientSessionClosure;
import com.hivemq.cluster.clientsession.ClientSessionOperation;
import com.hivemq.cluster.clientsession.ClientSessionService;

/**
 * 客户端断开请求处理类
 *
 * @author ankang
 * @since 2021/8/30
 */
public class ClientDisconnectRequestProcessor extends
        AbstractProcessor<ClientSessionOperation, ClientSessionResponse, ClientSessionClosure, ClientSessionService, ClientDisconnectRequest> {

    public ClientDisconnectRequestProcessor(final ClientSessionService raftService) {
        super(raftService);
    }

    @Override
    protected ClientSessionOperation transform(final ClientDisconnectRequest request) {
        final ClientSessionOperation operation = new ClientSessionOperation();
        operation.setType(ClientSessionOperation.Type.DISCONNECT);
        operation.setClientId(request.getClientId());
        operation.setHivemqId(request.getHivemqId());
        return operation;
    }

    @Override
    protected ClientSessionResponse createResponse() {
        return new ClientSessionResponse();
    }

    @Override
    protected ClientSessionClosure createClosure(
            final ClientSessionOperation request, final ClientSessionResponse response, final Closure done) {
        return new ClientSessionClosure(request, response, done);
    }

    @Override
    protected Class<ClientDisconnectRequest> getRequestClass() {
        return ClientDisconnectRequest.class;
    }
}
