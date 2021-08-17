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

import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.hivemq.cluster.AbstractStateMachine;
import com.hivemq.cluster.GroupIds;
import com.hivemq.cluster.clientsession.rpc.ClientSessionResponse;
import com.hivemq.cluster.ioc.SnapshotPersistence;
import com.hivemq.persistence.ProducerQueues;
import com.hivemq.persistence.SingleWriterService;
import com.hivemq.persistence.clientsession.ClientSession;
import com.hivemq.persistence.clientsession.ClientSessionPersistence;
import com.hivemq.persistence.local.ClientSessionLocalPersistence;
import com.hivemq.persistence.util.FutureUtils;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 客户端会话状态机
 *
 * @author ankang
 * @since 2021/8/11
 */
@Slf4j
@Singleton
public class ClientSessionStateMachine
        extends AbstractStateMachine<ClientSessionOperation, ClientSessionResponse, ClientSessionClosure> {

    private final ClientSessionPersistence clientSessionPersistence;
    private final ClientSessionLocalPersistence clientSessionLocalPersistence;
    private final ClientSessionLocalPersistence snapshotPersistence;
    private final ProducerQueues singleWriter;

    @Inject
    public ClientSessionStateMachine(
            final ClientSessionPersistence clientSessionPersistence,
            final ClientSessionLocalPersistence clientSessionLocalPersistence,
            final @SnapshotPersistence ClientSessionLocalPersistence snapshotPersistence,
            final SingleWriterService singleWriterService) {
        this.clientSessionPersistence = clientSessionPersistence;
        this.clientSessionLocalPersistence = clientSessionLocalPersistence;
        this.snapshotPersistence = snapshotPersistence;
        this.singleWriter = singleWriterService.getClientSessionQueue();
    }

    @Override
    protected Future<?> doApply(final ClientSessionOperation request) {
        Future<Void> future = null;
        switch (request.getType()) {
            case ADD:
                future = clientSessionPersistence.clientConnected(request.getClientId(),
                        request.isCleanStart(),
                        request.getSessionExpiryInterval(),
                        request.getWillPublish(),
                        request.getQueueLimit());
                break;
            case REMOVE:
                future = clientSessionPersistence.clientDisconnected(request.getClientId(),
                        request.isSendWill(),
                        request.getSessionExpiryInterval());
                break;
        }
        return future;
    }

    @Override
    protected void doSnapshotSave(final SnapshotWriter writer) throws Exception {
        transfer(clientSessionLocalPersistence, snapshotPersistence);
    }

    @Override
    protected void doSnapshotLoad(final SnapshotReader reader) throws Exception {
        transfer(snapshotPersistence, clientSessionLocalPersistence);
    }

    private void transfer(
            final ClientSessionLocalPersistence fromPersistence, final ClientSessionLocalPersistence toPersistence)
            throws InterruptedException, ExecutionException {
        // 当前存储和要转移的存储相同，忽略即可
        if (fromPersistence == toPersistence) {
            return;
        }
        final List<ListenableFuture<Void>> cleanupFutures =
                singleWriter.submitToAllQueues((bucketIndex, queueBuckets, queueIndex) -> {
                    for (final Integer bucket : queueBuckets) {
                        toPersistence.cleanUp(bucket);
                    }
                    return null;
                });
        FutureUtils.voidFutureFromList(ImmutableList.copyOf(cleanupFutures)).get();
        final ListenableFuture<List<Set<String>>> getAllFuture =
                singleWriter.submitToAllQueuesAsList((bucketIndex, queueBuckets, queueIndex) -> {
                    final Set<String> clientIds = new HashSet<>();
                    for (final Integer bucket : queueBuckets) {
                        clientIds.addAll(fromPersistence.getAllClients(bucket));
                    }
                    return clientIds;
                });
        final Set<String> clientIds = FutureUtils.combineSetResults(getAllFuture).get();
        for (final String clientId : clientIds) {
            final ClientSession session = fromPersistence.getSession(clientId);
            singleWriter.submit(clientId, (bucketIndex, queueBuckets, queueIndex) -> {
                final Long timestamp = fromPersistence.getTimestamp(clientId, bucketIndex);
                toPersistence.put(clientId, session, timestamp, bucketIndex);
                return null;
            });
        }
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
