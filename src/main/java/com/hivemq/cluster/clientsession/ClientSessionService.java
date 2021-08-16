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

package com.hivemq.cluster.clientsession;

import com.alipay.remoting.rpc.protocol.UserProcessor;
import com.hivemq.cluster.AbstractRaftService;
import com.hivemq.cluster.ClusterServerManager;
import com.hivemq.cluster.clientsession.rpc.ClientSessionAddRequestProcessor;
import com.hivemq.cluster.clientsession.rpc.ClientSessionRemoveRequestProcessor;
import com.hivemq.cluster.clientsession.rpc.ClientSessionResponse;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

/**
 * 客户端会话服务
 *
 * @author ankang
 * @since 2021/8/13
 */
@Singleton
public class ClientSessionService
        extends AbstractRaftService<ClientSessionOperation, ClientSessionResponse, ClientSessionClosure> {

    @Inject
    public ClientSessionService(
            final ClientSessionStateMachine stateMachine, final ClusterServerManager clusterServerManager) {
        super(stateMachine, clusterServerManager);
    }

    @Override
    protected Iterable<UserProcessor<?>> getProcessors() {
        final List<UserProcessor<?>> processors = new ArrayList<>();
        processors.add(new ClientSessionAddRequestProcessor(this));
        processors.add(new ClientSessionRemoveRequestProcessor(this));
        return processors;
    }
}
