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

package com.hivemq.persistence.ioc;

import com.hivemq.bootstrap.ioc.SingletonModule;
import com.hivemq.configuration.service.InternalConfigurations;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.migration.meta.PersistenceType;
import com.hivemq.persistence.clientqueue.ClientQueueLocalPersistence;
import com.hivemq.persistence.clientqueue.ClientQueueXodusLocalPersistence;
import com.hivemq.persistence.ioc.provider.local.ClientSessionLocalProvider;
import com.hivemq.persistence.ioc.provider.local.ClientSessionSubscriptionLocalProvider;
import com.hivemq.persistence.local.ClientSessionLocalPersistence;
import com.hivemq.persistence.local.ClientSessionSubscriptionLocalPersistence;

import javax.inject.Singleton;

/**
 * @author Florian Limpöck
 */
public class PersistenceMigrationFileModule extends SingletonModule<Class<PersistenceMigrationFileModule>> {

    private final @NotNull PersistenceType payloadPersistenceType;
    private final @NotNull PersistenceType retainedPersistenceType;

    public PersistenceMigrationFileModule() {
        super(PersistenceMigrationFileModule.class);
        this.payloadPersistenceType = InternalConfigurations.PAYLOAD_PERSISTENCE_TYPE.get();
        this.retainedPersistenceType = InternalConfigurations.RETAINED_MESSAGE_PERSISTENCE_TYPE.get();
    }

    @Override
    protected void configure() {

        bind(ClientSessionLocalPersistence.class).toProvider(ClientSessionLocalProvider.class).in(Singleton.class);
        bind(ClientSessionSubscriptionLocalPersistence.class).toProvider(ClientSessionSubscriptionLocalProvider.class).in(Singleton.class);
        bind(ClientQueueLocalPersistence.class).to(ClientQueueXodusLocalPersistence.class).in(Singleton.class);
    }
}
