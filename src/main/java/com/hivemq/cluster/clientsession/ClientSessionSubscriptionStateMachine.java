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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.hivemq.cluster.InternalStateMachine;
import com.hivemq.cluster.LocalPersistenceSnapshotSupport;
import com.hivemq.cluster.ioc.SnapshotPersistence;
import com.hivemq.extensions.iteration.BucketChunkResult;
import com.hivemq.mqtt.message.subscribe.Topic;
import com.hivemq.persistence.ProducerQueues;
import com.hivemq.persistence.SingleWriterService;
import com.hivemq.persistence.clientsession.ClientSessionSubscriptionPersistence;
import com.hivemq.persistence.local.ClientSessionSubscriptionLocalPersistence;
import com.hivemq.persistence.util.FutureUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * 客户端会话订阅状态机
 *
 * @author ankang
 * @since 2021/8/18
 */
@Singleton
public class ClientSessionSubscriptionStateMachine
        extends LocalPersistenceSnapshotSupport<ClientSessionSubscriptionLocalPersistence>
        implements InternalStateMachine<ClientSessionSubscriptionOperation> {

    private final ClientSessionSubscriptionPersistence clientSessionSubscriptionPersistence;
    private final ProducerQueues singleWriter;

    @Inject
    public ClientSessionSubscriptionStateMachine(
            final ClientSessionSubscriptionPersistence clientSessionSubscriptionPersistence,
            final ClientSessionSubscriptionLocalPersistence localPersistence,
            final @SnapshotPersistence ClientSessionSubscriptionLocalPersistence snapshotPersistence,
            final SingleWriterService singleWriterService) {
        super(localPersistence, snapshotPersistence);
        this.clientSessionSubscriptionPersistence = clientSessionSubscriptionPersistence;
        this.singleWriter = singleWriterService.getSubscriptionQueue();
    }

    @Override
    public Future<?> doApply(final ClientSessionSubscriptionOperation request) {
        Future<?> future = null;
        switch (request.getType()) {
            case ADD:
                future = clientSessionSubscriptionPersistence.addSubscriptions(request.getClientId(),
                        ImmutableSet.copyOf(request.getTopicObjects()));
                break;
            case REMOVE:
                future = clientSessionSubscriptionPersistence.removeSubscriptions(request.getClientId(),
                        ImmutableSet.copyOf(request.getTopicStrings()));
                break;
        }
        return future;
    }

    @Override
    protected void transfer(
            final ClientSessionSubscriptionLocalPersistence fromPersistence,
            final ClientSessionSubscriptionLocalPersistence toPersistence) throws Exception {
        final List<ListenableFuture<Void>> cleanupFutures =
                singleWriter.submitToAllQueues((bucketIndex, queueBuckets, queueIndex) -> {
                    for (final Integer bucket : queueBuckets) {
                        toPersistence.cleanUp(bucket);
                    }
                    return null;
                });
        FutureUtils.voidFutureFromList(ImmutableList.copyOf(cleanupFutures)).get();

        final List<ListenableFuture<Map<String, ImmutableSet<Topic>>>> futures =
                singleWriter.submitToAllQueues((bucketIndex, queueBuckets, queueIndex) -> {
                    final Map<String, ImmutableSet<Topic>> clientSessionSubscriptions = new HashMap<>();
                    for (final Integer bucket : queueBuckets) {
                        String lastClientId = null;
                        BucketChunkResult<Map<String, ImmutableSet<Topic>>> bucketChunkResult;
                        do {
                            bucketChunkResult =
                                    fromPersistence.getAllSubscribersChunk(bucket, lastClientId, Integer.MAX_VALUE);
                            clientSessionSubscriptions.putAll(bucketChunkResult.getValue());
                            lastClientId = bucketChunkResult.getLastKey();
                        } while (!bucketChunkResult.isFinished());
                    }
                    return clientSessionSubscriptions;
                });
        final Map<String, ImmutableSet<Topic>> clientSubscriptions =
                FutureUtils.combineMapResults(Futures.allAsList(futures)).get();

        final long timestamp = System.currentTimeMillis();
        for (final Map.Entry<String, ImmutableSet<Topic>> entry : clientSubscriptions.entrySet()) {
            final String clientId = entry.getKey();
            final ImmutableSet<Topic> topics = entry.getValue();
            singleWriter.submit(clientId, (bucketIndex, queueBuckets, queueIndex) -> {
                toPersistence.addSubscriptions(clientId, topics, timestamp, bucketIndex);
                return null;
            });
        }
    }
}
