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

package com.hivemq.cluster;

import com.hivemq.bootstrap.ioc.SingletonModule;
import com.hivemq.cluster.clientsession.ClientSessionClient;
import com.hivemq.cluster.clientsession.ClientSessionService;
import com.hivemq.cluster.clientsession.ClientSessionStateMachine;

import javax.inject.Singleton;

/**
 * 集群模块配置
 *
 * @author ankang
 * @since 2021/8/14
 */
public class ClusterModule extends SingletonModule<Class<ClusterModule>> {

    public ClusterModule() {
        super(ClusterModule.class);
    }

    @Override
    protected void configure() {
        bind(ClusterServerManager.class).asEagerSingleton();
        bind(ClientSessionStateMachine.class).in(Singleton.class);
        bind(ClientSessionService.class).in(Singleton.class);
        bind(ClientSessionClient.class).in(Singleton.class);
    }
}
