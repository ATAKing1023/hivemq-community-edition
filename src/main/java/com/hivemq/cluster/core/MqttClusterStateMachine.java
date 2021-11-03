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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.hivemq.cluster.AbstractStateMachine;
import com.hivemq.cluster.GroupIds;
import com.hivemq.cluster.clientqueue.ClientQueueOperation;
import com.hivemq.cluster.clientqueue.ClientQueueStateMachine;
import com.hivemq.cluster.clientsession.ClientSessionStateMachine;
import com.hivemq.cluster.clientsession.ClientSessionSubscriptionOperation;
import com.hivemq.cluster.clientsession.ClientSessionSubscriptionStateMachine;
import com.hivemq.mqtt.handler.publish.PublishStatus;
import com.hivemq.persistence.clientsession.callback.SubscriptionResult;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.concurrent.Future;

/**
 * MQTT集群操作状态机
 *
 * @author ankang
 * @since 2021/11/3
 */
@Slf4j
@Singleton
public class MqttClusterStateMachine
        extends AbstractStateMachine<MqttClusterRequest, MqttClusterResponse, MqttClusterClosure> {

    private final ClientSessionStateMachine clientSessionStateMachine;
    private final ClientSessionSubscriptionStateMachine clientSessionSubscriptionStateMachine;
    private final ClientQueueStateMachine clientQueueStateMachine;

    @Inject
    public MqttClusterStateMachine(
            final ClientSessionStateMachine clientSessionStateMachine,
            final ClientSessionSubscriptionStateMachine clientSessionSubscriptionStateMachine,
            final ClientQueueStateMachine clientQueueStateMachine) {
        this.clientSessionStateMachine = clientSessionStateMachine;
        this.clientSessionSubscriptionStateMachine = clientSessionSubscriptionStateMachine;
        this.clientQueueStateMachine = clientQueueStateMachine;
    }

    @Override
    protected Future<?> doApply(final MqttClusterRequest request) {
        if (request.getClientSessionOperation() != null) {
            return clientSessionStateMachine.doApply(request.getClientSessionOperation());
        }
        if (request.getClientSessionSubscriptionOperation() != null) {
            return clientSessionSubscriptionStateMachine.doApply(request.getClientSessionSubscriptionOperation());
        }
        if (request.getClientQueueOperation() != null) {
            return clientQueueStateMachine.doApply(request.getClientQueueOperation());
        }
        return null;
    }

    @Override
    public void onSnapshotSave(
            final SnapshotWriter writer, final Closure done) {
        clientSessionStateMachine.onSnapshotSave(writer, done);
        clientSessionSubscriptionStateMachine.onSnapshotSave(writer, done);
        clientQueueStateMachine.onSnapshotSave(writer, done);
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        return clientSessionStateMachine.onSnapshotLoad(reader) &&
                clientSessionSubscriptionStateMachine.onSnapshotLoad(reader) &&
                clientQueueStateMachine.onSnapshotLoad(reader);
    }

    @Override
    protected void setResponseData(final MqttClusterClosure closure, final Object result) {
        log.info("Closure: {}, result: {}", closure, result);
        if (closure.getRequest().getClientSessionSubscriptionOperation() != null) {
            if (closure.getRequest().getClientSessionSubscriptionOperation().getType() ==
                    ClientSessionSubscriptionOperation.Type.ADD) {
                closure.getResponse().setSubscriptionResults((List<SubscriptionResult>) result);
            }
        }
        if (closure.getRequest().getClientQueueOperation() != null) {
            if (closure.getRequest().getClientQueueOperation().getType() == ClientQueueOperation.Type.PUBLISH) {
                closure.getResponse().setPublishStatus((PublishStatus) result);
            }
        }
    }

    @Override
    protected Class<MqttClusterRequest> getRequestClass() {
        return MqttClusterRequest.class;
    }

    @Override
    public String getGroupId() {
        return GroupIds.MQTT_CLUSTER;
    }
}
