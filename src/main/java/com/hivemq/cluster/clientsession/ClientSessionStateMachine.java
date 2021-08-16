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

import com.hivemq.cluster.AbstractStateMachine;
import com.hivemq.cluster.GroupIds;
import com.hivemq.cluster.clientsession.rpc.ClientSessionResponse;
import com.hivemq.persistence.clientsession.ClientSessionPersistence;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.Future;

/**
 * 客户端会话状态机
 *
 * @author ankang
 * @since 2021/8/11
 */
@Singleton
public class ClientSessionStateMachine extends AbstractStateMachine<ClientSessionOperation, ClientSessionResponse, ClientSessionClosure> {

    private final ClientSessionPersistence clientSessionPersistence;

    @Inject
    public ClientSessionStateMachine(final ClientSessionPersistence clientSessionPersistence) {
        this.clientSessionPersistence = clientSessionPersistence;
    }

    @Override
    protected Future<?> doApply(final ClientSessionOperation request) {
        Future<Void> future = null;
        switch (request.getType()) {
            case ADD:
                future = clientSessionPersistence.clientConnected(
                        request.getClientId(),
                        request.isCleanStart(),
                        request.getSessionExpiryInterval(),
                        request.getWillPublish(),
                        request.getQueueLimit());
                break;
            case REMOVE:
                future = clientSessionPersistence.clientDisconnected(
                        request.getClientId(),
                        request.isSendWill(),
                        request.getSessionExpiryInterval());
                break;
        }
        return future;
    }

    @Override
    protected Class<ClientSessionOperation> getRequestClass() {
        return ClientSessionOperation.class;
    }

    @Override
    public String getGroupId() {
        return GroupIds.CLIENT_SESSION;
    }
}
