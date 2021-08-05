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

import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.options.RocksDBOptions;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.util.Endpoint;
import com.hivemq.exceptions.UnrecoverableException;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.persistence.FilePersistence;
import com.hivemq.persistence.LocalPersistence;
import com.hivemq.persistence.PersistenceStartup;
import com.hivemq.persistence.local.xodus.bucket.BucketUtils;
import com.hivemq.util.LocalPersistenceFileUtil;
import org.slf4j.Logger;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author ankang
 */
public abstract class RheaKVLocalPersistence implements LocalPersistence, FilePersistence {

    protected static final int DEFAULT_BUFFER_SIZE = 64;

    protected final AtomicBoolean stopped = new AtomicBoolean(false);
    protected final @NotNull RheaKVStore[] buckets;
    private final @NotNull LocalPersistenceFileUtil localPersistenceFileUtil;
    private final @NotNull PersistenceStartup persistenceStartup;
    private final int bucketCount;
    private final boolean enabled;

    private final RheaKVStoreOptions rheaKVStoreOptions;

    protected RheaKVLocalPersistence(
            final @NotNull LocalPersistenceFileUtil localPersistenceFileUtil,
            final @NotNull PersistenceStartup persistenceStartup,
            final int internalBucketCount,
            final boolean enabled) {
        this.bucketCount = internalBucketCount;
        this.buckets = new RheaKVStore[bucketCount];
        this.localPersistenceFileUtil = localPersistenceFileUtil;
        this.persistenceStartup = persistenceStartup;
        this.enabled = enabled;
        this.rheaKVStoreOptions = createRheaKVStoreOptions();
    }

    @NotNull
    protected abstract String getName();

    @NotNull
    protected abstract String getVersion();

    @NotNull
    protected abstract Logger getLogger();

    public int getBucketCount() {
        return bucketCount;
    }

    protected void postConstruct() {
        if (enabled) {
            persistenceStartup.submitPersistenceStart(this);
        } else {
            startExternal();
        }
    }

    @Override
    public void startExternal() {
        final String name = getName();
        final Logger logger = getLogger();

        for (int i = 0; i < bucketCount; i++) {
            final RheaKVStore rheaKVStore = new DefaultRheaKVStore();
            final boolean success = rheaKVStore.init(rheaKVStoreOptions);
            if (!success) {
                logger.error("An error occurred while opening the {} persistence. Is another HiveMQ instance running?",
                        name);
                throw new UnrecoverableException();
            }
            buckets[i] = rheaKVStore;
        }

        init();
    }

    @Override
    public void start() {
        final String name = getName();
        final Logger logger = getLogger();

        try {
            final CountDownLatch counter = new CountDownLatch(bucketCount);
            for (int i = 0; i < bucketCount; i++) {
                final int finalI = i;
                persistenceStartup.submitEnvironmentCreate(() -> {
                    final RheaKVStore rheaKVStore = new DefaultRheaKVStore();
                    final boolean success = rheaKVStore.init(rheaKVStoreOptions);
                    if (!success) {
                        logger.error(
                                "An error occurred while opening the {} persistence. Is another HiveMQ instance running?",
                                name);
                        throw new UnrecoverableException();
                    }
                    buckets[finalI] = rheaKVStore;
                    counter.countDown();
                });
            }
            counter.await();
        } catch (final Exception e) {
            logger.error("An error occurred while opening the {} persistence. Is another HiveMQ instance running?",
                    name);
            logger.info("Original Exception:", e);
            throw new UnrecoverableException();
        }

        init();
    }

    protected abstract void init();

    @Override
    public void stop() {
        stopped.set(true);
        closeDB();
    }

    public void closeDB() {
        for (int i = 0; i < bucketCount; i++) {
            closeDB(i);
        }
    }

    @Override
    public void closeDB(final int bucketIndex) {
        checkBucketIndex(bucketIndex);
        final RheaKVStore bucket = buckets[bucketIndex];
        bucket.shutdown();
    }

    @NotNull
    protected RheaKVStore getRheaKVStore(final @NotNull String key) {
        return buckets[BucketUtils.getBucket(key, bucketCount)];
    }

    @NotNull
    protected int getBucketIndex(final @NotNull String key) {
        return BucketUtils.getBucket(key, bucketCount);
    }

    protected void checkBucketIndex(final int bucketIndex) {
        checkArgument(bucketIndex >= 0 && bucketIndex < buckets.length, "Invalid bucket index: " + bucketIndex);
    }

    private RheaKVStoreOptions createRheaKVStoreOptions() {
        final File persistenceFolder =
                localPersistenceFileUtil.getVersionedLocalPersistenceFolder(getName(), getVersion());

        final RheaKVStoreOptions options = new RheaKVStoreOptions();
        options.setClusterId(1L);
        options.setClusterName("HiveMQ");
        final PlacementDriverOptions pdOptions = new PlacementDriverOptions();
        pdOptions.setFake(true);
        options.setPlacementDriverOptions(pdOptions);
        final StoreEngineOptions storeEngineOptions = new StoreEngineOptions();
        final RocksDBOptions rocksDBOptions = new RocksDBOptions();
        rocksDBOptions.setSync(true);
        rocksDBOptions.setDbPath(new File(persistenceFolder, "rhea_db/").getPath());
        storeEngineOptions.setRocksDBOptions(rocksDBOptions);
        storeEngineOptions.setRaftDataPath(new File(persistenceFolder, "rhea_raft/").getPath());
        storeEngineOptions.setServerAddress(new Endpoint("127.0.0.1", 8181));
        storeEngineOptions.setInitialServerList("127.0.0.1:8181");
        options.setStoreEngineOptions(storeEngineOptions);
        options.setOnlyLeaderRead(false);
        options.setFailoverRetries(2);
        return options;
    }
}
