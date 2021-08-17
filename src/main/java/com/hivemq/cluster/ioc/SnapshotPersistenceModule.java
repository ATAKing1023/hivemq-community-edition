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

package com.hivemq.cluster.ioc;

import com.google.inject.Injector;
import com.hivemq.bootstrap.ioc.SingletonModule;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.persistence.clientqueue.ClientQueueLocalPersistence;
import com.hivemq.persistence.clientqueue.ClientQueueXodusLocalPersistence;
import com.hivemq.persistence.ioc.provider.local.ClientSessionLocalProvider;
import com.hivemq.persistence.ioc.provider.local.ClientSessionSubscriptionLocalProvider;
import com.hivemq.persistence.local.ClientSessionLocalPersistence;
import com.hivemq.persistence.local.ClientSessionSubscriptionLocalPersistence;
import com.hivemq.persistence.local.xodus.RetainedMessageXodusLocalPersistence;
import com.hivemq.persistence.local.xodus.clientsession.ClientSessionSubscriptionXodusLocalPersistence;
import com.hivemq.persistence.local.xodus.clientsession.ClientSessionXodusLocalPersistence;
import com.hivemq.persistence.payload.PublishPayloadLocalPersistence;
import com.hivemq.persistence.payload.PublishPayloadXodusLocalPersistence;
import com.hivemq.persistence.retained.RetainedMessageLocalPersistence;

import javax.inject.Singleton;

/**
 * @author ankang
 */
public class SnapshotPersistenceModule extends SingletonModule<Class<SnapshotPersistenceModule>> {

    private final Injector injector;

    public SnapshotPersistenceModule(final Injector injector) {
        super(SnapshotPersistenceModule.class);
        this.injector = injector;
    }

    @Override
    protected void configure() {
        bindLocalPersistence(PublishPayloadLocalPersistence.class, PublishPayloadXodusLocalPersistence.class, null);
        bindLocalPersistence(RetainedMessageLocalPersistence.class, RetainedMessageXodusLocalPersistence.class, null);

        bindLocalPersistence(
                ClientSessionLocalPersistence.class,
                ClientSessionXodusLocalPersistence.class,
                ClientSessionLocalProvider.class);

        bindLocalPersistence(
                ClientSessionSubscriptionLocalPersistence.class,
                ClientSessionSubscriptionXodusLocalPersistence.class,
                ClientSessionSubscriptionLocalProvider.class);

        bindLocalPersistence(ClientQueueLocalPersistence.class, ClientQueueXodusLocalPersistence.class, null);
    }

    private void bindLocalPersistence(
            final @NotNull Class localPersistenceClass,
            final @NotNull Class localPersistenceImplClass,
            final @Nullable Class localPersistenceProviderClass) {
        final Object instance = injector == null ? null : injector.getInstance(localPersistenceImplClass);
        if (instance != null) {
            bind(localPersistenceClass).annotatedWith(SnapshotPersistence.class).toInstance(instance);
        } else {
            if (localPersistenceProviderClass != null) {
                bind(localPersistenceClass).annotatedWith(SnapshotPersistence.class)
                        .toProvider(localPersistenceProviderClass)
                        .in(Singleton.class);
            } else {
                bind(localPersistenceClass).annotatedWith(SnapshotPersistence.class)
                        .to(localPersistenceImplClass)
                        .in(Singleton.class);
            }
        }
    }
}
