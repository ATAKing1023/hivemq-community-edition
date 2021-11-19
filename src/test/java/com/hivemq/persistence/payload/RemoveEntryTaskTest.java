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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hivemq.cluster.HazelcastManager;
import com.hivemq.configuration.HivemqId;
import com.hivemq.mqtt.message.publish.PUBLISH;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

/**
 * @author Lukas Brandl
 */
public class RemoveEntryTaskTest {

    @Mock
    private PublishPayloadLocalPersistence localPersistence;
    @Mock
    private HazelcastManager hazelcastManager;

    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();

    private Cache<String, byte[]> payloadCache;
    private BucketLock bucketLock;
    private Queue<RemovablePayload> removablePayloads;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        payloadCache = CacheBuilder.newBuilder()
                .concurrencyLevel(1)
                .build();
        bucketLock = new BucketLock(1);
        removablePayloads = new LinkedTransferQueue<>();

        when(hazelcastManager.getReferenceCount(anyString())).then(invocation ->
                hazelcastInstance.getCPSubsystem().getAtomicLong(invocation.getArgument(0)));
    }

    @Test
    public void test_no_remove_during_delay() throws Exception {
        removablePayloads.add(new RemovablePayload(PUBLISH.getUniqueId(HivemqId.get(), 1), System.currentTimeMillis()));
        payloadCache.put(PUBLISH.getUniqueId(HivemqId.get(), 1), "test".getBytes());
        hazelcastManager.getReferenceCount(PUBLISH.getUniqueId(HivemqId.get(), 1)).set(0);
        final RemoveEntryTask task = new RemoveEntryTask(payloadCache, localPersistence, bucketLock, removablePayloads, 10000L, hazelcastManager, 10000);
        task.run();
        assertNotNull(payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 1)));
        assertEquals(1, removablePayloads.size());
    }

    @Test
    public void test_no_remove_if_refcount_not_zero() throws Exception {
        removablePayloads.add(new RemovablePayload(PUBLISH.getUniqueId(HivemqId.get(), 1), System.currentTimeMillis() - 100L));
        payloadCache.put(PUBLISH.getUniqueId(HivemqId.get(), 1), "test".getBytes());
        hazelcastManager.getReferenceCount(PUBLISH.getUniqueId(HivemqId.get(), 1)).set(1);
        final RemoveEntryTask task = new RemoveEntryTask(payloadCache, localPersistence, bucketLock, removablePayloads, 10L, hazelcastManager, 10000);
        task.run();
        assertNotNull(payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 1)));
        assertEquals(0, removablePayloads.size());
    }

    @Test
    public void test_remove_after_delay() throws Exception {
        removablePayloads.add(new RemovablePayload(PUBLISH.getUniqueId(HivemqId.get(), 1), System.currentTimeMillis() - 100L));
        payloadCache.put(PUBLISH.getUniqueId(HivemqId.get(), 1), "test".getBytes());
        hazelcastManager.getReferenceCount(PUBLISH.getUniqueId(HivemqId.get(), 1)).set(0);
        final RemoveEntryTask task = new RemoveEntryTask(payloadCache, localPersistence, bucketLock, removablePayloads, 10L, hazelcastManager, 10000);
        task.run();
        assertNull(payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 1)));
        assertEquals(0, removablePayloads.size());
    }

    @Test
    public void test_both() throws Exception {
        removablePayloads.add(new RemovablePayload(PUBLISH.getUniqueId(HivemqId.get(), 1), System.currentTimeMillis() - 100000L));
        removablePayloads.add(new RemovablePayload(PUBLISH.getUniqueId(HivemqId.get(), 2), System.currentTimeMillis()));
        payloadCache.put(PUBLISH.getUniqueId(HivemqId.get(), 1), "test".getBytes());
        payloadCache.put(PUBLISH.getUniqueId(HivemqId.get(), 2), "test".getBytes());
        hazelcastManager.getReferenceCount(PUBLISH.getUniqueId(HivemqId.get(), 1)).set(0);
        hazelcastManager.getReferenceCount(PUBLISH.getUniqueId(HivemqId.get(), 2)).set(0);
        final RemoveEntryTask task = new RemoveEntryTask(payloadCache, localPersistence, bucketLock, removablePayloads, 10000L, hazelcastManager, 10000);
        task.run();
        assertNull(payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 1)));
        assertNotNull(payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 2)));
        assertEquals(1, removablePayloads.size());
    }

    @Test
    public void test_remove_if_marked_twice() throws Exception {
        removablePayloads.add(new RemovablePayload(PUBLISH.getUniqueId(HivemqId.get(), 1), System.currentTimeMillis() - 100L));
        removablePayloads.add(new RemovablePayload(PUBLISH.getUniqueId(HivemqId.get(), 1), System.currentTimeMillis() - 500L));
        payloadCache.put(PUBLISH.getUniqueId(HivemqId.get(), 1), "test".getBytes());
        hazelcastManager.getReferenceCount(PUBLISH.getUniqueId(HivemqId.get(), 1)).set(0);
        final RemoveEntryTask task = new RemoveEntryTask(payloadCache, localPersistence, bucketLock, removablePayloads, 10L, hazelcastManager, 10000);
        task.run();
        assertNull(payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 1)));
        assertEquals(0, removablePayloads.size());
    }

    @Test(timeout = 5000)
    public void test_dont_stop_in_case_of_exception() throws Exception {
        removablePayloads.add(new RemovablePayload(PUBLISH.getUniqueId(HivemqId.get(), 1), System.currentTimeMillis() - 100L));
        removablePayloads.add(new RemovablePayload(PUBLISH.getUniqueId(HivemqId.get(), 1), System.currentTimeMillis() - 100L));
        payloadCache.put(PUBLISH.getUniqueId(HivemqId.get(), 1), "test".getBytes());
        hazelcastManager.getReferenceCount(PUBLISH.getUniqueId(HivemqId.get(), 1)).set(0);
        doThrow(new RuntimeException("expected")).doNothing().when(localPersistence).remove(anyString());
        final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        final RemoveEntryTask task = new RemoveEntryTask(payloadCache, localPersistence, bucketLock, removablePayloads, 10L, hazelcastManager, 10000);
        executorService.scheduleAtFixedRate(task, 10, 10, TimeUnit.MILLISECONDS);

        while (payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 1)) != null || removablePayloads.size() > 0) {
            Thread.sleep(10);
        }

        assertNull(payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 1)));
        assertEquals(0, removablePayloads.size());
        executorService.shutdown();
    }
}