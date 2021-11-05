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
package com.hivemq.persistence.payload;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.hivemq.bootstrap.ioc.lazysingleton.LazySingleton;
import com.hivemq.configuration.service.InternalConfigurations;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.migration.meta.PersistenceType;
import com.hivemq.persistence.PersistenceStartup;
import com.hivemq.persistence.local.rocksdb.RocksDBLocalPersistence;
import com.hivemq.util.LocalPersistenceFileUtil;
import com.hivemq.util.PhysicalMemoryUtil;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.hivemq.persistence.payload.PublishPayloadRocksDBSerializer.deserializeKey;
import static com.hivemq.persistence.payload.PublishPayloadRocksDBSerializer.serializeKey;

/**
 * @author Florian Limpöck
 */
@LazySingleton
public class PublishPayloadRocksDBLocalPersistence extends RocksDBLocalPersistence implements PublishPayloadLocalPersistence {

    @VisibleForTesting
    static final Logger log = LoggerFactory.getLogger(PublishPayloadRocksDBLocalPersistence.class);
    private final FlushOptions FLUSH_OPTIONS = new FlushOptions().setAllowWriteStall(true); // must not be gc´d

    public static final String PERSISTENCE_VERSION = "040500_R";
    private final long memtableSize;
    private final boolean forceFlush;


    @NotNull
    private long[] rocksdbToMemTableSize;

    @Inject
    public PublishPayloadRocksDBLocalPersistence(final @NotNull LocalPersistenceFileUtil localPersistenceFileUtil,
                                                 final @NotNull PersistenceStartup persistenceStartup) {
        super(localPersistenceFileUtil,
                persistenceStartup,
                InternalConfigurations.PAYLOAD_PERSISTENCE_BUCKET_COUNT.get(),
                InternalConfigurations.PAYLOAD_PERSISTENCE_MEMTABLE_SIZE_PORTION.get(),
                InternalConfigurations.PAYLOAD_PERSISTENCE_BLOCK_CACHE_SIZE_PORTION.get(),
                InternalConfigurations.PAYLOAD_PERSISTENCE_BLOCK_SIZE,
                InternalConfigurations.PAYLOAD_PERSISTENCE_TYPE.get() == PersistenceType.FILE_NATIVE);
        this.memtableSize = PhysicalMemoryUtil.physicalMemory() / InternalConfigurations.PAYLOAD_PERSISTENCE_MEMTABLE_SIZE_PORTION.get()
                / InternalConfigurations.PAYLOAD_PERSISTENCE_BUCKET_COUNT.get();
        this.rocksdbToMemTableSize = new long[InternalConfigurations.PAYLOAD_PERSISTENCE_BUCKET_COUNT.get()];
        this.forceFlush = InternalConfigurations.PUBLISH_PAYLOAD_FORCE_FLUSH.get();
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
    protected Logger getLogger() {
        return log;
    }

    @Override
    protected @NotNull Options getOptions() {
        return new Options()
                .setCreateIfMissing(true)
                .setStatistics(new Statistics());
    }

    @Override
    @PostConstruct
    protected void postConstruct() {
        super.postConstruct();
    }

    @Override
    public void init() {
    }

    @Override
    public void put(final String id, @NotNull final byte[] payload) {
        checkNotNull(payload, "payload must not be null");
        final int index = getBucketIndex(id);
        final RocksDB bucket = buckets[index];
        ;
        try {
            bucket.put(serializeKey(id), payload);
            if (forceFlush) {
                flushOnMemtableOverflow(bucket, index, payload.length);
            }
        } catch (final RocksDBException e) {
            log.error("Could not put a payload because of an exception: ", e);
        }
    }

    private void flushOnMemtableOverflow(final @NotNull RocksDB bucket, final int bucketIndex, final int payloadSize) throws RocksDBException {
        final long updatedSize = payloadSize + rocksdbToMemTableSize[bucketIndex];
        if (updatedSize >= memtableSize) {
            bucket.flush(FLUSH_OPTIONS);
            if (log.isDebugEnabled()) {
                log.debug("Hard flushing memTable due to exceeding memTable limit {}.", memtableSize);
            }
            rocksdbToMemTableSize[bucketIndex] = 0L;
        } else {
            rocksdbToMemTableSize[bucketIndex] = updatedSize;
        }
    }

    @Nullable
    @Override
    public byte[] get(final String id) {
        final RocksDB bucket = getRocksDb(id);
        try {
            return bucket.get(serializeKey(id));
        } catch (final RocksDBException e) {
            log.error("Could not get a payload because of an exception: ", e);
        }
        return null;
    }

    @NotNull
    @Override
    public ImmutableList<String> getAllIds() {

        final ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (final RocksDB bucket : buckets) {
            try(final RocksIterator rocksIterator = bucket.newIterator()) {
                rocksIterator.seekToFirst();
                while (rocksIterator.isValid()) {
                    final byte[] key = rocksIterator.key();
                    builder.add(deserializeKey(key));
                    rocksIterator.next();
                }
            }
        }

        return builder.build();
    }

    @Override
    public void remove(final String id) {
        if (stopped.get()) {
            return;
        }
        final RocksDB bucket = getRocksDb(id);
        try {
            bucket.delete(serializeKey(id));
        } catch (final RocksDBException e) {
            log.error("Could not delete a payload because of an exception: ", e);
        }
    }


    @Override
    public void iterate(final @NotNull Callback callback) {
        for (final RocksDB bucket : buckets) {
            try(final RocksIterator rocksIterator = bucket.newIterator()) {
                rocksIterator.seekToFirst();
                while (rocksIterator.isValid()) {
                    final String payloadId = deserializeKey(rocksIterator.key());
                    callback.call(payloadId, rocksIterator.value());
                    rocksIterator.next();
                }
            }
        }
    }


    @VisibleForTesting
    long[] getRocksdbToMemTableSize() {
        return rocksdbToMemTableSize;
    }

    public long getMemtableSize() {
        return memtableSize;
    }

}
