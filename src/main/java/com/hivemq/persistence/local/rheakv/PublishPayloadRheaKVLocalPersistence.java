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
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.hivemq.bootstrap.ioc.lazysingleton.LazySingleton;
import com.hivemq.configuration.service.InternalConfigurations;
import com.hivemq.exceptions.UnrecoverableException;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.migration.meta.PersistenceType;
import com.hivemq.persistence.PersistenceStartup;
import com.hivemq.persistence.payload.PublishPayloadLocalPersistence;
import com.hivemq.persistence.payload.PublishPayloadRocksDBLocalPersistence;
import com.hivemq.util.LocalPersistenceFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.hivemq.persistence.payload.PublishPayloadRocksDBSerializer.deserializeKey;
import static com.hivemq.persistence.payload.PublishPayloadRocksDBSerializer.serializeKey;

/**
 * @author ankang
 */
@LazySingleton
public class PublishPayloadRheaKVLocalPersistence extends RheaKVLocalPersistence
        implements PublishPayloadLocalPersistence {

    @VisibleForTesting
    static final Logger log = LoggerFactory.getLogger(PublishPayloadRheaKVLocalPersistence.class);

    public static final String PERSISTENCE_VERSION = PublishPayloadRocksDBLocalPersistence.PERSISTENCE_VERSION;

    private long maxId = 0;

    @Inject
    public PublishPayloadRheaKVLocalPersistence(
            final @NotNull LocalPersistenceFileUtil localPersistenceFileUtil,
            final @NotNull PersistenceStartup persistenceStartup) {
        super(
                localPersistenceFileUtil,
                persistenceStartup,
                InternalConfigurations.PAYLOAD_PERSISTENCE_BUCKET_COUNT.get(),
                InternalConfigurations.PAYLOAD_PERSISTENCE_TYPE.get() == PersistenceType.FILE_DISTRIBUTED);
    }

    @Override
    @NotNull
    protected String getName() {
        return PERSISTENCE_NAME;
    }

    @Override
    @NotNull
    protected String getVersion() {
        return PERSISTENCE_VERSION;
    }

    @Override
    @NotNull
    protected ContentType getContentType() {
        return ContentType.PUBLISH_PAYLOAD;
    }

    @Override
    @NotNull
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
            long max = 0;
            for (final RheaKVStore bucket : buckets) {
                final RheaIterator<KVEntry> iterator = bucket.iterator((byte[]) null, null, DEFAULT_BUFFER_SIZE);
                while (iterator.hasNext()) {
                    final KVEntry entry = iterator.next();
                    final long key = deserializeKey(entry.getKey());
                    if (key > max) {
                        max = key;
                    }
                }
            }
            maxId = max;
        } catch (final Exception e) {
            log.error("An error occurred while preparing the Publish Payload persistence.");
            log.debug("Original Exception:", e);
            throw new UnrecoverableException(false);
        }
    }

    @Override
    public void put(final long id, @NotNull final byte[] payload) {
        checkNotNull(payload, "payload must not be null");

        final int index = getBucketIndex(Long.toString(id));
        final RheaKVStore bucket = buckets[index];
        try {
            bucket.bPut(serializeKey(id), payload);
        } catch (final Exception e) {
            log.error("Could not put a payload because of an exception: ", e);
        }
    }

    @Nullable
    @Override
    public byte[] get(final long id) {
        final RheaKVStore bucket = getRheaKVStore(Long.toString(id));
        try {
            return bucket.bGet(serializeKey(id));
        } catch (final Exception e) {
            log.error("Could not get a payload because of an exception: ", e);
        }
        return null;
    }

    @NotNull
    @Override
    public ImmutableList<Long> getAllIds() {
        final ImmutableList.Builder<Long> builder = ImmutableList.builder();
        for (final RheaKVStore bucket : buckets) {
            final RheaIterator<KVEntry> iterator = bucket.iterator((byte[]) null, null, DEFAULT_BUFFER_SIZE);
            while (iterator.hasNext()) {
                final KVEntry entry = iterator.next();
                builder.add(deserializeKey(entry.getKey()));
            }
        }
        return builder.build();
    }

    @Override
    public void remove(final long id) {
        if (stopped.get()) {
            return;
        }
        final RheaKVStore bucket = getRheaKVStore(Long.toString(id));
        try {
            bucket.bDelete(serializeKey(id));
        } catch (final Exception e) {
            log.error("Could not delete a payload because of an exception: ", e);
        }
    }

    @Override
    public void iterate(final @NotNull Callback callback) {
        for (final RheaKVStore bucket : buckets) {
            final RheaIterator<KVEntry> iterator = bucket.iterator((byte[]) null, null, DEFAULT_BUFFER_SIZE);
            while (iterator.hasNext()) {
                final KVEntry entry = iterator.next();
                final long payloadId = deserializeKey(entry.getKey());
                callback.call(payloadId, entry.getValue());
            }
        }
    }

    @Override
    public long getMaxId() {
        return maxId;
    }
}
