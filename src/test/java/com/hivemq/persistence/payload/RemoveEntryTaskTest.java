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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.hivemq.configuration.HivemqId;
import com.hivemq.mqtt.message.publish.PUBLISH;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;

/**
 * @author Lukas Brandl
 */
public class RemoveEntryTaskTest {

    @Mock
    private PublishPayloadLocalPersistence localPersistence;

    private Cache<String, byte[]> payloadCache;
    private BucketLock bucketLock;
    private Queue<RemovablePayload> removablePayloads;
    private final ConcurrentHashMap<String, AtomicLong> referenceCounter = new ConcurrentHashMap<>();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        payloadCache = CacheBuilder.newBuilder()
                .concurrencyLevel(1)
                .build();
        bucketLock = new BucketLock(1);
        removablePayloads = new LinkedTransferQueue<>();
    }

    @Test
    public void test_no_remove_during_delay() throws Exception {
        removablePayloads.add(new RemovablePayload(PUBLISH.getUniqueId(HivemqId.get(), 1), System.currentTimeMillis()));
        payloadCache.put(PUBLISH.getUniqueId(HivemqId.get(), 1), "test".getBytes());
        referenceCounter.put(PUBLISH.getUniqueId(HivemqId.get(), 1), new AtomicLong(0));
        final RemoveEntryTask task = new RemoveEntryTask(payloadCache, localPersistence, bucketLock, removablePayloads, 10000L, referenceCounter, 10000);
        task.run();
        assertNotNull(payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 1)));
        assertEquals(1, removablePayloads.size());
        assertEquals(1, referenceCounter.size());
    }

    @Test
    public void test_no_remove_if_refcount_not_zero() throws Exception {
        removablePayloads.add(new RemovablePayload(PUBLISH.getUniqueId(HivemqId.get(), 1), System.currentTimeMillis() - 100L));
        payloadCache.put(PUBLISH.getUniqueId(HivemqId.get(), 1), "test".getBytes());
        referenceCounter.put(PUBLISH.getUniqueId(HivemqId.get(), 1), new AtomicLong(1));
        final RemoveEntryTask task = new RemoveEntryTask(payloadCache, localPersistence, bucketLock, removablePayloads, 10L, referenceCounter, 10000);
        task.run();
        assertNotNull(payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 1)));
        assertEquals(0, removablePayloads.size());
        assertEquals(1, referenceCounter.size());
    }

    @Test
    public void test_remove_after_delay() throws Exception {
        removablePayloads.add(new RemovablePayload(PUBLISH.getUniqueId(HivemqId.get(), 1), System.currentTimeMillis() - 100L));
        payloadCache.put(PUBLISH.getUniqueId(HivemqId.get(), 1), "test".getBytes());
        referenceCounter.put(PUBLISH.getUniqueId(HivemqId.get(), 1), new AtomicLong(0));
        final RemoveEntryTask task = new RemoveEntryTask(payloadCache, localPersistence, bucketLock, removablePayloads, 10L, referenceCounter, 10000);
        task.run();
        assertNull(payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 1)));
        assertEquals(0, removablePayloads.size());
        assertEquals(0, referenceCounter.size());
    }

    @Test
    public void test_both() throws Exception {
        removablePayloads.add(new RemovablePayload(PUBLISH.getUniqueId(HivemqId.get(), 1), System.currentTimeMillis() - 100000L));
        removablePayloads.add(new RemovablePayload(PUBLISH.getUniqueId(HivemqId.get(), 2), System.currentTimeMillis()));
        payloadCache.put(PUBLISH.getUniqueId(HivemqId.get(), 1), "test".getBytes());
        payloadCache.put(PUBLISH.getUniqueId(HivemqId.get(), 2), "test".getBytes());
        referenceCounter.put(PUBLISH.getUniqueId(HivemqId.get(), 1), new AtomicLong(0));
        referenceCounter.put(PUBLISH.getUniqueId(HivemqId.get(), 2), new AtomicLong(0));
        final RemoveEntryTask task = new RemoveEntryTask(payloadCache, localPersistence, bucketLock, removablePayloads, 10000L, referenceCounter, 10000);
        task.run();
        assertNull(payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 1)));
        assertNotNull(payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 2)));
        assertEquals(1, removablePayloads.size());
        assertEquals(1, referenceCounter.size());
    }

    @Test
    public void test_remove_if_marked_twice() throws Exception {
        removablePayloads.add(new RemovablePayload(PUBLISH.getUniqueId(HivemqId.get(), 1), System.currentTimeMillis() - 100L));
        removablePayloads.add(new RemovablePayload(PUBLISH.getUniqueId(HivemqId.get(), 1), System.currentTimeMillis() - 500L));
        payloadCache.put(PUBLISH.getUniqueId(HivemqId.get(), 1), "test".getBytes());
        referenceCounter.put(PUBLISH.getUniqueId(HivemqId.get(), 1), new AtomicLong(0));
        final RemoveEntryTask task = new RemoveEntryTask(payloadCache, localPersistence, bucketLock, removablePayloads, 10L, referenceCounter, 10000);
        task.run();
        assertNull(payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 1)));
        assertEquals(0, removablePayloads.size());
        assertEquals(0, referenceCounter.size());
    }

    @Test(timeout = 5000)
    public void test_dont_stop_in_case_of_exception() throws Exception {
        removablePayloads.add(new RemovablePayload(PUBLISH.getUniqueId(HivemqId.get(), 1), System.currentTimeMillis() - 100L));
        removablePayloads.add(new RemovablePayload(PUBLISH.getUniqueId(HivemqId.get(), 1), System.currentTimeMillis() - 100L));
        payloadCache.put(PUBLISH.getUniqueId(HivemqId.get(), 1), "test".getBytes());
        referenceCounter.put(PUBLISH.getUniqueId(HivemqId.get(), 1), new AtomicLong(0));
        doThrow(new RuntimeException("expected")).doNothing().when(localPersistence).remove(anyString());
        final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        final RemoveEntryTask task = new RemoveEntryTask(payloadCache, localPersistence, bucketLock, removablePayloads, 10L, referenceCounter, 10000);
        executorService.scheduleAtFixedRate(task, 10, 10, TimeUnit.MILLISECONDS);

        while (payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 1)) != null || removablePayloads.size() > 0 || referenceCounter.size() > 0) {
            Thread.sleep(10);
        }

        assertNull(payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 1)));
        assertEquals(0, removablePayloads.size());
        assertEquals(0, referenceCounter.size());
        executorService.shutdown();
    }
}