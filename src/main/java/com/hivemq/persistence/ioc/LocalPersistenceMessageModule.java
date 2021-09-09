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

import com.google.inject.Injector;
import com.hivemq.bootstrap.ioc.SingletonModule;
import com.hivemq.configuration.service.InternalConfigurations;
import com.hivemq.configuration.service.PersistenceConfigurationService;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.migration.meta.PersistenceType;
import com.hivemq.persistence.local.memory.PublishPayloadMemoryLocalPersistence;
import com.hivemq.persistence.local.memory.RetainedMessageMemoryLocalPersistence;
import com.hivemq.persistence.local.rheakv.PublishPayloadRheaKVLocalPersistence;
import com.hivemq.persistence.local.rheakv.RetainedMessageRheaKVLocalPersistence;
import com.hivemq.persistence.local.xodus.RetainedMessageRocksDBLocalPersistence;
import com.hivemq.persistence.local.xodus.RetainedMessageXodusLocalPersistence;
import com.hivemq.persistence.payload.PublishPayloadLocalPersistence;
import com.hivemq.persistence.payload.PublishPayloadRocksDBLocalPersistence;
import com.hivemq.persistence.payload.PublishPayloadXodusLocalPersistence;
import com.hivemq.persistence.retained.RetainedMessageLocalPersistence;

import javax.inject.Singleton;

/**
 * @author ankang
 */
class LocalPersistenceMessageModule extends SingletonModule<Class<LocalPersistenceMessageModule>> {

    private final Injector persistenceInjector;
    private final PersistenceConfigurationService.PersistenceMode persistenceMode;
    private final PersistenceType payloadPersistenceType;
    private final PersistenceType retainedPersistenceType;

    LocalPersistenceMessageModule(
            @NotNull final Injector persistenceInjector, @NotNull final PersistenceConfigurationService.PersistenceMode persistenceMode) {
        super(LocalPersistenceMessageModule.class);
        this.persistenceInjector = persistenceInjector;
        this.persistenceMode = persistenceMode;
        this.payloadPersistenceType = InternalConfigurations.PAYLOAD_PERSISTENCE_TYPE.get();
        this.retainedPersistenceType = InternalConfigurations.RETAINED_MESSAGE_PERSISTENCE_TYPE.get();
    }

    @Override
    protected void configure() {

        if (payloadPersistenceType == PersistenceType.FILE_DISTRIBUTED) {
            bindLocalPersistence(PublishPayloadLocalPersistence.class, PublishPayloadRheaKVLocalPersistence.class);
        } else if (persistenceMode == PersistenceConfigurationService.PersistenceMode.IN_MEMORY) {
            bindLocalPersistence(PublishPayloadLocalPersistence.class, PublishPayloadMemoryLocalPersistence.class);
        } else {
            if (payloadPersistenceType == PersistenceType.FILE) {
                bindLocalPersistence(PublishPayloadLocalPersistence.class, PublishPayloadXodusLocalPersistence.class);
            } else if (payloadPersistenceType == PersistenceType.FILE_NATIVE) {
                bindLocalPersistence(PublishPayloadLocalPersistence.class, PublishPayloadRocksDBLocalPersistence.class);
            }
        }

        if (retainedPersistenceType == PersistenceType.FILE_DISTRIBUTED) {
            bindLocalPersistence(RetainedMessageLocalPersistence.class, RetainedMessageRheaKVLocalPersistence.class);
        } else if (persistenceMode == PersistenceConfigurationService.PersistenceMode.IN_MEMORY) {
            bindLocalPersistence(RetainedMessageLocalPersistence.class, RetainedMessageMemoryLocalPersistence.class);
        } else {
            if (retainedPersistenceType == PersistenceType.FILE) {
                bindLocalPersistence(RetainedMessageLocalPersistence.class, RetainedMessageXodusLocalPersistence.class);
            } else if (retainedPersistenceType == PersistenceType.FILE_NATIVE) {
                bindLocalPersistence(RetainedMessageLocalPersistence.class, RetainedMessageRocksDBLocalPersistence.class);
            }
        }
    }

    private void bindLocalPersistence(
            final @NotNull Class localPersistenceClass, final @NotNull Class localPersistenceImplClass) {

        final Object instance = persistenceInjector == null ? null : persistenceInjector.getInstance(localPersistenceImplClass);
        if (instance != null) {
            bind(localPersistenceImplClass).toInstance(instance);
            bind(localPersistenceClass).toInstance(instance);
        } else {
            bind(localPersistenceClass).to(localPersistenceImplClass).in(Singleton.class);
        }
    }
}
