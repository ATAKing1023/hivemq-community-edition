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

import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hivemq.cluster.HazelcastManager;
import com.hivemq.configuration.service.InternalConfigurations;
import com.hivemq.mqtt.message.publish.PUBLISH;
import net.openhft.hashing.LongHashFunction;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import util.LogbackCapturingAppender;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Lukas Brandl
 */
public class PublishPayloadPersistenceImplTest {

    @Mock
    PublishPayloadLocalPersistence localPersistence;
    @Mock
    ListeningScheduledExecutorService scheduledExecutorService;

    private final LongHashFunction hashFunction = LongHashFunction.xx();

    PublishPayloadPersistenceImpl persistence;

    private LogbackCapturingAppender logCapture;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        InternalConfigurations.PAYLOAD_CACHE_DURATION.set(1000L);
        InternalConfigurations.PAYLOAD_CACHE_SIZE.set(1000);
        InternalConfigurations.PAYLOAD_CACHE_CONCURRENCY_LEVEL.set(1);
        InternalConfigurations.PAYLOAD_PERSISTENCE_CLEANUP_SCHEDULE.set(10000);
        InternalConfigurations.PAYLOAD_PERSISTENCE_BUCKET_COUNT.set(64);

        persistence = new PublishPayloadPersistenceImpl(localPersistence, scheduledExecutorService);
        persistence.init();
        logCapture = LogbackCapturingAppender.Factory.weaveInto(PublishPayloadPersistenceImpl.log);
    }

    @After
    public void tearDown() throws Exception {
        LogbackCapturingAppender.Factory.cleanUp();
    }

    @Test
    public void add_new_entries() throws Exception {
        final String hivemqId = RandomStringUtils.randomAlphanumeric(5);
        final byte[] payload1 = "payload1".getBytes();
        final byte[] payload2 = "payload2".getBytes();
        persistence.add(payload1, 1, PUBLISH.getUniqueId(HivemqId.get(), 123));
        persistence.add(payload2, 2, PUBLISH.getUniqueId(HivemqId.get(), 234));

        assertEquals(1, persistence.referenceCounter.get(PUBLISH.getUniqueId(HivemqId.get(), 123)).get());
        assertEquals(2, persistence.referenceCounter.get(PUBLISH.getUniqueId(HivemqId.get(), 234)).get());
        assertNotNull(persistence.payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 123)));
        assertNotNull(persistence.payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 234)));
    }

    @Test
    public void add_existent_entry() throws Exception {
        final String hivemqId = RandomStringUtils.randomAlphanumeric(5);
        final byte[] payload = "payload".getBytes();
        persistence.add(payload, 1, PUBLISH.getUniqueId(HivemqId.get(), 123));
        persistence.add(payload, 2, PUBLISH.getUniqueId(HivemqId.get(), 123));


        assertEquals(3, persistence.referenceCounter.get(PUBLISH.getUniqueId(HivemqId.get(), 123)).get());
        assertNotNull(persistence.payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 123)));
        assertEquals(1, persistence.payloadCache.size());
    }

    @Test
    public void get_from_cache() throws Exception {
        final String hivemqId = RandomStringUtils.randomAlphanumeric(5);
        final byte[] payload = "payload".getBytes();
        persistence.add(payload, 1, PUBLISH.getUniqueId(HivemqId.get(), 123));

        final long hash = hashFunction.hashBytes(payload);

        assertEquals(1, persistence.referenceCounter.get(PUBLISH.getUniqueId(HivemqId.get(), 123)).get());
        assertNotNull(persistence.payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 123)));
        assertEquals(1, persistence.payloadCache.size());

        final byte[] result = persistence.get(PUBLISH.getUniqueId(HivemqId.get(), 123));

        verify(localPersistence, never()).get(anyString());
        assertTrue(Arrays.equals(payload, result));
    }

    @Test
    public void get_from_local_persistence() throws Exception {
        final String hivemqId = RandomStringUtils.randomAlphanumeric(5);
        final byte[] payload = "payload".getBytes();
        persistence.add(payload, 1, PUBLISH.getUniqueId(HivemqId.get(), 123));

        when(localPersistence.get(PUBLISH.getUniqueId(HivemqId.get(), 123))).thenReturn(payload);
        persistence.payloadCache.invalidate(PUBLISH.getUniqueId(HivemqId.get(), 123));

        assertEquals(1, persistence.referenceCounter.get(PUBLISH.getUniqueId(HivemqId.get(), 123)).get());
        assertNull(persistence.payloadCache.getIfPresent(PUBLISH.getUniqueId(HivemqId.get(), 123)));
        assertEquals(0, persistence.payloadCache.size());

        final byte[] result = persistence.get(PUBLISH.getUniqueId(HivemqId.get(), 123));

        verify(localPersistence, times(1)).get(anyString());
        assertTrue(Arrays.equals(payload, result));
    }

    @Test(expected = PayloadPersistenceException.class)
    public void get_from_local_persistence_null_payload() throws Exception {
        persistence.get(PUBLISH.getUniqueId(HivemqId.get(), 1));
    }

    @Test
    public void get_from_local_persistence_retained_message_null_payload() throws Exception {
        final byte[] bytes = persistence.getPayloadOrNull(PUBLISH.getUniqueId(HivemqId.get(), 1));
        assertNull(bytes);
    }

    @Test
    public void increment_new_reference_count() throws Exception {
        persistence.incrementReferenceCounterOnBootstrap(PUBLISH.getUniqueId(HivemqId.get(), 0));
        assertEquals(1L, persistence.referenceCounter.get(PUBLISH.getUniqueId(HivemqId.get(), 0)).get());
    }

    @Test
    public void increment_existing_reference_count() throws Exception {
        persistence.referenceCounter.put(PUBLISH.getUniqueId(HivemqId.get(), 0), new AtomicLong(1L));
        persistence.incrementReferenceCounterOnBootstrap(PUBLISH.getUniqueId(HivemqId.get(), 0));
        assertEquals(2L, persistence.referenceCounter.get(PUBLISH.getUniqueId(HivemqId.get(), 0)).get());
    }

    @Test
    public void decrement_reference_count() throws Exception {
        persistence.referenceCounter.put(PUBLISH.getUniqueId(HivemqId.get(), 0), new AtomicLong(2L));
        persistence.decrementReferenceCounter(PUBLISH.getUniqueId(HivemqId.get(), 0));
        assertEquals(1L, persistence.referenceCounter.get(PUBLISH.getUniqueId(HivemqId.get(), 0)).get());
        assertEquals(0, persistence.removablePayloads.size());
    }

    @Test
    public void decrement_reference_count_to_zero() throws Exception {
        persistence.referenceCounter.put(PUBLISH.getUniqueId(HivemqId.get(), 0), new AtomicLong(1L));
        persistence.decrementReferenceCounter(PUBLISH.getUniqueId(HivemqId.get(), 0));
        assertEquals(0L, persistence.referenceCounter.get(PUBLISH.getUniqueId(HivemqId.get(), 0)).get());
        assertEquals(1, persistence.removablePayloads.size());
    }

    @Test
    public void decrement_reference_count_already_zero() throws Exception {
        persistence.referenceCounter.put(PUBLISH.getUniqueId(HivemqId.get(), 0), new AtomicLong(0L));
        persistence.decrementReferenceCounter(PUBLISH.getUniqueId(HivemqId.get(), 0));
        assertEquals(0L, persistence.referenceCounter.get(PUBLISH.getUniqueId(HivemqId.get(), 0)).get());
        assertEquals(0, persistence.removablePayloads.size());
    }

    @Test
    public void decrement_reference_count_null() throws Exception {
        persistence.decrementReferenceCounter(PUBLISH.getUniqueId(HivemqId.get(), 0));
        assertNull(persistence.referenceCounter.get(PUBLISH.getUniqueId(HivemqId.get(), 0)));
        assertEquals(0, persistence.removablePayloads.size());
    }

    @Test
    public void init_persistence() throws Exception {

        InternalConfigurations.PAYLOAD_PERSISTENCE_CLEANUP_SCHEDULE.set(250);
        InternalConfigurations.PAYLOAD_PERSISTENCE_CLEANUP_THREADS.set(4);

        persistence = new PublishPayloadPersistenceImpl(localPersistence, scheduledExecutorService);

        persistence.init();

        verify(scheduledExecutorService).scheduleAtFixedRate(any(RemoveEntryTask.class), eq(0L), eq(250L * 4L), eq(TimeUnit.MILLISECONDS));
        verify(scheduledExecutorService).scheduleAtFixedRate(any(RemoveEntryTask.class), eq(250L), eq(250L * 4L), eq(TimeUnit.MILLISECONDS));
        verify(scheduledExecutorService).scheduleAtFixedRate(any(RemoveEntryTask.class), eq(500L), eq(250L * 4L), eq(TimeUnit.MILLISECONDS));
        verify(scheduledExecutorService).scheduleAtFixedRate(any(RemoveEntryTask.class), eq(750L), eq(250L * 4L), eq(TimeUnit.MILLISECONDS));
    }

}