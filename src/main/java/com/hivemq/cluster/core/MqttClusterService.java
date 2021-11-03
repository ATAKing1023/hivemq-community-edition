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

package com.hivemq.cluster.core;

import com.alipay.remoting.rpc.protocol.UserProcessor;
import com.hivemq.cluster.AbstractRaftService;
import com.hivemq.cluster.ClusterServerManager;
import com.hivemq.cluster.clientqueue.rpc.ClientQueuePublishRequestProcessor;
import com.hivemq.cluster.clientsession.rpc.*;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

/**
 * MQTT集群操作服务
 *
 * @author ankang
 * @since 2021/11/3
 */
@Singleton
public class MqttClusterService extends AbstractRaftService<MqttClusterRequest, MqttClusterResponse, MqttClusterClosure> {

    @Inject
    protected MqttClusterService(
            final MqttClusterStateMachine stateMachine, final ClusterServerManager clusterServerManager) {
        super(stateMachine, clusterServerManager);
    }

    @Override
    protected Iterable<UserProcessor<?>> getProcessors() {
        final List<UserProcessor<?>> processors = new ArrayList<>();
        processors.add(new ClientSessionAddRequestProcessor(this));
        processors.add(new ClientSessionRemoveRequestProcessor(this));
        processors.add(new ClientDisconnectRequestProcessor(this));
        processors.add(new ClientSessionSubscriptionAddRequestProcessor(this));
        processors.add(new ClientSessionSubscriptionRemoveRequestProcessor(this));
        processors.add(new ClientQueuePublishRequestProcessor(this));
        return processors;
    }
}
