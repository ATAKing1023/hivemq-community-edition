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

import com.hivemq.cluster.AbstractRpcClient;
import com.hivemq.cluster.ClusterServerManager;
import com.hivemq.cluster.GroupIds;
import com.hivemq.cluster.clientsession.rpc.ClientSessionResponse;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * 客户端会话操作客户端
 *
 * @author ankang
 * @since 2021/8/16
 */
@Singleton
public class ClientSessionRpcClient extends AbstractRpcClient<ClientSessionResponse> {

    @Inject
    public ClientSessionRpcClient(final ClusterServerManager clusterServerManager) {
        super(clusterServerManager);
    }

    @Override
    public String getGroupId() {
        return GroupIds.CLIENT_SESSION;
    }
}
