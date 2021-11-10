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
import com.hivemq.cluster.ClusterServerManager;
import com.hivemq.cluster.HazelcastManager;
import com.hivemq.cluster.clientqueue.ClientQueueStateMachine;
import com.hivemq.cluster.clientsession.ClientSessionStateMachine;
import com.hivemq.cluster.clientsession.ClientSessionSubscriptionStateMachine;
import com.hivemq.cluster.core.MqttClusterClient;
import com.hivemq.cluster.core.MqttClusterService;
import com.hivemq.cluster.core.MqttClusterStateMachine;

import javax.inject.Singleton;

/**
 * 集群模块配置
 *
 * @author ankang
 * @since 2021/8/14
 */
public class ClusterModule extends SingletonModule<Class<ClusterModule>> {

    private final Injector persistenceInjector;

    public ClusterModule(final Injector persistenceInjector) {
        super(ClusterModule.class);
        this.persistenceInjector = persistenceInjector;
    }

    @Override
    protected void configure() {
        install(new SnapshotPersistenceModule(persistenceInjector));

        bind(ClusterServerManager.class).asEagerSingleton();

        bind(ClientSessionStateMachine.class).in(Singleton.class);
        bind(ClientSessionSubscriptionStateMachine.class).in(Singleton.class);
        bind(ClientQueueStateMachine.class).in(Singleton.class);
        bind(MqttClusterStateMachine.class).in(Singleton.class);
        bind(MqttClusterService.class).in(Singleton.class);
        bind(MqttClusterClient.class).in(Singleton.class);
    }
}
