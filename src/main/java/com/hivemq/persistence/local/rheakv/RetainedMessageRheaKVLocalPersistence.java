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

package com.hivemq.persistence.local.rheakv;

import com.alipay.sofa.jraft.rhea.client.RheaIterator;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.hivemq.bootstrap.ioc.lazysingleton.LazySingleton;
import com.hivemq.cluster.ClusterServerManager;
import com.hivemq.configuration.service.InternalConfigurations;
import com.hivemq.exceptions.UnrecoverableException;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.extensions.iteration.BucketChunkResult;
import com.hivemq.migration.meta.PersistenceType;
import com.hivemq.persistence.PersistenceStartup;
import com.hivemq.persistence.RetainedMessage;
import com.hivemq.persistence.local.xodus.PublishTopicTree;
import com.hivemq.persistence.local.xodus.RetainedMessageRocksDBLocalPersistence;
import com.hivemq.persistence.local.xodus.RetainedMessageXodusSerializer;
import com.hivemq.persistence.payload.PublishPayloadPersistence;
import com.hivemq.persistence.retained.RetainedMessageLocalPersistence;
import com.hivemq.util.LocalPersistenceFileUtil;
import com.hivemq.util.PublishUtil;
import com.hivemq.util.ThreadPreConditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.hivemq.util.ThreadPreConditions.SINGLE_WRITER_THREAD_PREFIX;

/**
 * @author ankang
 */
@LazySingleton
public class RetainedMessageRheaKVLocalPersistence extends RheaKVLocalPersistence
        implements RetainedMessageLocalPersistence {

    private static final Logger log = LoggerFactory.getLogger(RetainedMessageRheaKVLocalPersistence.class);

    public static final String PERSISTENCE_VERSION = RetainedMessageRocksDBLocalPersistence.PERSISTENCE_VERSION;
    @VisibleForTesting
    public final @NotNull PublishTopicTree[] topicTrees;
    private final @NotNull PublishPayloadPersistence payloadPersistence;
    private final @NotNull RetainedMessageXodusSerializer serializer;
    private final @NotNull AtomicLong retainMessageCounter = new AtomicLong(0);

    @Inject
    public RetainedMessageRheaKVLocalPersistence(
            final @NotNull LocalPersistenceFileUtil localPersistenceFileUtil,
            final @NotNull PublishPayloadPersistence payloadPersistence,
            final @NotNull PersistenceStartup persistenceStartup,
            final @NotNull ClusterServerManager clusterServerManager) {
        super(localPersistenceFileUtil,
                persistenceStartup,
                clusterServerManager,
                InternalConfigurations.PERSISTENCE_BUCKET_COUNT.get(),
                InternalConfigurations.RETAINED_MESSAGE_PERSISTENCE_TYPE.get() == PersistenceType.FILE_DISTRIBUTED);

        this.payloadPersistence = payloadPersistence;
        this.serializer = new RetainedMessageXodusSerializer();
        final int bucketCount = getBucketCount();
        this.topicTrees = new PublishTopicTree[bucketCount];
        for (int i = 0; i < bucketCount; i++) {
            topicTrees[i] = new PublishTopicTree();
        }
    }

    @NotNull
    @Override
    protected String getName() {
        return PERSISTENCE_NAME;
    }

    @NotNull
    @Override
    protected String getVersion() {
        return PERSISTENCE_VERSION;
    }

    @NotNull
    @Override
    protected ContentType getContentType() {
        return ContentType.RETAINED_MESSAGE;
    }

    @NotNull
    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    @PostConstruct
    protected void postConstruct() {
        super.postConstruct();
    }

    @Override
    public void init() {
        try {
            for (int i = 0; i < buckets.length; i++) {
                final RheaKVStore bucket = buckets[i];
                final RheaIterator<KVEntry> iterator = bucket.iterator((byte[]) null, null, DEFAULT_BUFFER_SIZE);
                while (iterator.hasNext()) {
                    final KVEntry entry = iterator.next();
                    final RetainedMessage message = serializer.deserializeValue(entry.getValue());
                    payloadPersistence.incrementReferenceCounterOnBootstrap(message.getUniqueId());
                    final String topic = serializer.deserializeKey(entry.getKey());
                    topicTrees[i].add(topic);
                    retainMessageCounter.incrementAndGet();
                }
            }
        } catch (final Exception e) {
            log.error("An error occurred while preparing the Retained Message persistence.", e);
            throw new UnrecoverableException(false);
        }
    }

    @Override
    public void bootstrapPayloads() {
        try {
            for (final RheaKVStore bucket : buckets) {
                final RheaIterator<KVEntry> iterator = bucket.iterator((byte[]) null, null, DEFAULT_BUFFER_SIZE);
                while (iterator.hasNext()) {
                    final KVEntry entry = iterator.next();
                    final RetainedMessage message = serializer.deserializeValue(entry.getValue());
                    payloadPersistence.incrementReferenceCounterOnBootstrap(message.getUniqueId());
                }
            }
        } catch (final Exception e) {
            log.error("An error occurred while preparing the Retained Message persistence.", e);
            throw new UnrecoverableException(false);
        }
    }

    @Override
    public void clear(final int bucketIndex) {
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);

        topicTrees[bucketIndex] = new PublishTopicTree();
        final RheaKVStore bucket = buckets[bucketIndex];
        try {
            final List<byte[]> keys = new ArrayList<>();
            final RheaIterator<KVEntry> iterator = bucket.iterator((byte[]) null, null, DEFAULT_BUFFER_SIZE);
            while (iterator.hasNext()) {
                final KVEntry entry = iterator.next();
                keys.add(entry.getKey());
                final RetainedMessage message = serializer.deserializeValue(entry.getValue());
                payloadPersistence.decrementReferenceCounter(message.getUniqueId());
                retainMessageCounter.decrementAndGet();
            }
            if (!keys.isEmpty()) {
                bucket.bDelete(keys);
            }
        } catch (final Exception e) {
            log.error("An error occurred while clearing the retained message persistence.", e);
        }
    }

    @Override
    public long size() {
        return retainMessageCounter.get();
    }

    @Override
    public void remove(@NotNull final String topic, final int bucketIndex) {
        checkNotNull(topic, "Topic must not be null");
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);

        final RheaKVStore bucket = buckets[bucketIndex];
        try {
            final byte[] key = serializer.serializeKey(topic);
            final byte[] removed = bucket.bGet(key);
            if (removed == null) {
                log.trace("Removing retained message for topic {} (no message was stored previously)", topic);
                return;
            }

            final RetainedMessage message = serializer.deserializeValue(removed);

            log.trace("Removing retained message for topic {}", topic);
            bucket.bDelete(key);
            topicTrees[bucketIndex].remove(topic);
            payloadPersistence.decrementReferenceCounter(message.getUniqueId());
            retainMessageCounter.decrementAndGet();
        } catch (final Exception e) {
            log.error("An error occurred while removing a retained message.", e);
        }
    }

    @Nullable
    @Override
    public RetainedMessage get(@NotNull final String topic, final int bucketIndex) {
        try {
            return tryGetLocally(topic, 0, bucketIndex);
        } catch (final Exception e) {
            log.error("An error occurred while getting a retained message.", e);
            return null;
        }
    }

    private RetainedMessage tryGetLocally(@NotNull final String topic, final int retry, final int bucketIndex)
            throws Exception {
        checkNotNull(topic, "Topic must not be null");
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);

        final RheaKVStore bucket = buckets[bucketIndex];
        final byte[] messageAsBytes = bucket.bGet(serializer.serializeKey(topic));
        if (messageAsBytes != null) {
            final RetainedMessage message = serializer.deserializeValue(messageAsBytes);
            final byte[] payload = payloadPersistence.getPayloadOrNull(message.getUniqueId());
            if (payload == null) {
                // In case the payload was just deleted, we return the new retained message for this topic (or null if it was removed).
                if (retry < 100) {
                    return tryGetLocally(topic, retry + 1, bucketIndex);
                } else {
                    log.warn("No payload was found for the retained message on topic {}.", topic);
                    return null;
                }
            }

            if (PublishUtil.checkExpiry(message.getTimestamp(), message.getMessageExpiryInterval())) {
                return null;
            }
            message.setMessage(payload);
            return message;
        }
        return null;
    }

    @Override
    public void put(
            @NotNull final RetainedMessage retainedMessage, @NotNull final String topic, final int bucketIndex) {
        checkNotNull(topic, "Topic must not be null");
        checkNotNull(retainedMessage, "Retained message must not be null");
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);

        final RheaKVStore bucket = buckets[bucketIndex];
        try {
            final byte[] serializedTopic = serializer.serializeKey(topic);
            final byte[] valueAsBytes = bucket.bGet(serializedTopic);
            if (valueAsBytes != null) {
                final RetainedMessage retainedMessageFromStore = serializer.deserializeValue(valueAsBytes);
                log.trace("Replacing retained message for topic {}", topic);
                bucket.bPut(serializedTopic, serializer.serializeValue(retainedMessage));
                // The previous retained message is replaced, so we have to decrement the reference count.
                payloadPersistence.decrementReferenceCounter(retainedMessageFromStore.getUniqueId());
            } else {
                log.trace("Creating new retained message for topic {}", topic);
                bucket.bPut(serializedTopic, serializer.serializeValue(retainedMessage));
                topicTrees[bucketIndex].add(topic);
                //persist needs increment.
                retainMessageCounter.incrementAndGet();
            }
        } catch (final Exception e) {
            log.error("An error occurred while persisting a retained message.", e);
        }

    }

    @NotNull
    @Override
    public Set<String> getAllTopics(
            @NotNull final String subscription, final int bucketId) {
        checkArgument(bucketId >= 0 && bucketId < getBucketCount(), "Bucket index out of range");
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);
        return topicTrees[bucketId].get(subscription);
    }

    @Override
    public void cleanUp(final int bucketId) {
        checkArgument(bucketId >= 0 && bucketId < getBucketCount(), "Bucket index out of range");
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);
        if (stopped.get()) {
            return;
        }

        final RheaKVStore bucket = buckets[bucketId];
        final PublishTopicTree topicTree = topicTrees[bucketId];
        try {
            final List<byte[]> keys = new ArrayList<>();
            final RheaIterator<KVEntry> iterator = bucket.iterator((byte[]) null, null, DEFAULT_BUFFER_SIZE);
            while (iterator.hasNext()) {
                final KVEntry entry = iterator.next();
                final String topic = serializer.deserializeKey(entry.getKey());
                final RetainedMessage message = serializer.deserializeValue(entry.getValue());
                if (PublishUtil.checkExpiry(message.getTimestamp(), message.getMessageExpiryInterval())) {
                    keys.add(entry.getKey());
                    payloadPersistence.decrementReferenceCounter(message.getUniqueId());
                    retainMessageCounter.decrementAndGet();
                    topicTree.remove(topic);
                }
            }
            if (!keys.isEmpty()) {
                bucket.bDelete(keys);
            }
        } catch (final Exception e) {
            log.error("An error occurred while cleaning up retained messages.", e);
        }
    }

    @Override
    public @NotNull BucketChunkResult<Map<String, @NotNull RetainedMessage>> getAllRetainedMessagesChunk(
            final int bucketIndex, final @Nullable String lastTopic, final int maxMemory) {
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);

        final RheaKVStore bucket = buckets[bucketIndex];
        RheaIterator<KVEntry> iterator;
        if (lastTopic == null) {
            iterator = bucket.iterator((byte[]) null, null, DEFAULT_BUFFER_SIZE);
        } else {
            iterator = bucket.iterator(serializer.serializeKey(lastTopic), null, DEFAULT_BUFFER_SIZE);
            // we already have this one, lets look for the next
            if (iterator.hasNext()) {
                final KVEntry entry = iterator.next();
                final String deserializedTopic = serializer.deserializeKey(entry.getKey());
                // we double check that in between calls no messages were removed
                if (!deserializedTopic.equals(lastTopic)) {
                    iterator = bucket.iterator(serializer.serializeKey(lastTopic), null, DEFAULT_BUFFER_SIZE);
                }
            }
        }

        int usedMemory = 0;
        final ImmutableMap.Builder<String, RetainedMessage> retrievedMessages = ImmutableMap.builder();
        String lastFoundTopic = null;

        // we iterate either until the end of the persistence or until the maximum requested messages are found
        while (iterator.hasNext() && usedMemory < maxMemory) {
            final KVEntry entry = iterator.next();
            final String deserializedTopic = serializer.deserializeKey(entry.getKey());
            final RetainedMessage deserializedMessage = serializer.deserializeValue(entry.getValue());

            // ignore messages with exceeded message expiry interval
            if (PublishUtil.checkExpiry(deserializedMessage.getTimestamp(),
                    deserializedMessage.getMessageExpiryInterval())) {
                continue;
            }

            final byte[] payload = payloadPersistence.getPayloadOrNull(deserializedMessage.getUniqueId());

            // ignore messages with no payload and log a warning for the fact
            if (payload == null) {
                log.warn(
                        "Could not dereference payload for retained message on topic \"{}\" with payload id \"{}\".",
                        deserializedTopic,
                        deserializedMessage.getUniqueId());
                continue;
            }
            deserializedMessage.setMessage(payload);

            lastFoundTopic = deserializedTopic;
            usedMemory += deserializedMessage.getEstimatedSizeInMemory();

            retrievedMessages.put(lastFoundTopic, deserializedMessage);
        }

        // if the iterator is not valid any more we know that there is nothing more to get
        return new BucketChunkResult<>(retrievedMessages.build(), !iterator.hasNext(), lastFoundTopic, bucketIndex);
    }

    @Override
    public void iterate(final @NotNull ItemCallback callback) {
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);

        for (final RheaKVStore bucket : buckets) {
            final RheaIterator<KVEntry> iterator = bucket.iterator((byte[]) null, null, DEFAULT_BUFFER_SIZE);
            while (iterator.hasNext()) {
                final KVEntry entry = iterator.next();
                final String topic = serializer.deserializeKey(entry.getKey());
                final RetainedMessage message = serializer.deserializeValue(entry.getValue());
                callback.onItem(topic, message);
            }
        }
    }

}
