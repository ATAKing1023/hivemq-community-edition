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

package com.hivemq.cluster.clientqueue;

import com.hivemq.cluster.GroupIds;
import com.hivemq.cluster.LocalPersistenceBasedStateMachine;
import com.hivemq.cluster.clientqueue.rpc.ClientQueueResponse;
import com.hivemq.cluster.ioc.SnapshotPersistence;
import com.hivemq.mqtt.handler.publish.PublishStatus;
import com.hivemq.mqtt.services.PublishDistributor;
import com.hivemq.persistence.clientqueue.ClientQueueLocalPersistence;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.Future;

/**
 * 客户端队列状态机
 *
 * @author ankang
 * @since 2021/9/3
 */
@Singleton
public class ClientQueueStateMachine extends
        LocalPersistenceBasedStateMachine<ClientQueueLocalPersistence, ClientQueueOperation, ClientQueueResponse, ClientQueueClosure> {

    private final PublishDistributor publishDistributor;

    @Inject
    public ClientQueueStateMachine(
            final PublishDistributor publishDistributor,
            final ClientQueueLocalPersistence localPersistence,
            final @SnapshotPersistence ClientQueueLocalPersistence snapshotPersistence) {
        super(localPersistence, snapshotPersistence);
        this.publishDistributor = publishDistributor;
    }

    @Override
    protected Future<?> doApply(final ClientQueueOperation request) {
        Future<?> future = null;
        switch (request.getType()) {
            case PUBLISH:
                future = publishDistributor.sendMessageToSubscriber(
                        request.getPublish(),
                        request.getClient(),
                        request.getSubscriptionQos(),
                        request.isShared(),
                        request.isRetainAsPublished(),
                        request.getSubscriptionIdentifier());
                break;
        }
        return future;
    }

    @Override
    protected void setResponseData(final ClientQueueClosure closure, final Object result) {
        if (closure.getRequest().getType() == ClientQueueOperation.Type.PUBLISH) {
            closure.getResponse().setPublishStatus((PublishStatus) result);
        }
    }

    @Override
    protected Class<ClientQueueOperation> getRequestClass() {
        return ClientQueueOperation.class;
    }

    @Override
    public String getGroupId() {
        return GroupIds.CLIENT_QUEUE;
    }

    @Override
    protected void transfer(
            final ClientQueueLocalPersistence fromPersistence, final ClientQueueLocalPersistence toPersistence)
            throws Exception {

    }
}
