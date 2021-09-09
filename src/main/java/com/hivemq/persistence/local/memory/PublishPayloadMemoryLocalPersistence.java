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

package com.hivemq.persistence.local.memory;

import com.google.common.collect.ImmutableList;
import com.hivemq.configuration.service.InternalConfigurations;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.persistence.local.xodus.bucket.BucketUtils;
import com.hivemq.persistence.payload.PublishPayloadLocalPersistence;

import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

/**
 * 基于内存的消息内容本地存储
 *
 * @author ankang
 * @since 2021/9/7
 */
@Singleton
public class PublishPayloadMemoryLocalPersistence implements PublishPayloadLocalPersistence {

    private final Map<Long, byte[]>[] buckets;
    private final int bucketCount;

    public PublishPayloadMemoryLocalPersistence() {
        bucketCount = InternalConfigurations.PERSISTENCE_BUCKET_COUNT.get();
        //noinspection unchecked
        buckets = new HashMap[bucketCount];
        for (int i = 0; i < bucketCount; i++) {
            buckets[i] = new HashMap<>();
        }
    }

    @Override
    public void init() {

    }

    @Override
    public void put(final long id, @NotNull final byte[] payload) {
        getBucket(id).put(id, payload);
    }

    @Override
    public @Nullable byte[] get(final long id) {
        return getBucket(id).getOrDefault(id, new byte[0]);
    }

    @Override
    public void remove(final long id) {
        getBucket(id).remove(id);
    }

    @Override
    public long getMaxId() {
        // always 0
        return 0;
    }

    @Override
    public ImmutableList<Long> getAllIds() {
        final ImmutableList.Builder<Long> builder = ImmutableList.builder();
        for (final Map<Long, byte[]> bucket : buckets) {
            builder.addAll(bucket.keySet());
        }
        return builder.build();
    }

    @Override
    public void closeDB() {
        // noop
    }

    @Override
    public void iterate(final Callback callback) {
        throw new UnsupportedOperationException(
                "Iterate is only used for migrations which are not needed for memory persistences");
    }

    private Map<Long, byte[]> getBucket(final long id) {
        return buckets[BucketUtils.getBucket(String.valueOf(id), bucketCount)];
    }

    @Override
    public void closeDB(int bucketIndex) {
        // noop
    }
}
