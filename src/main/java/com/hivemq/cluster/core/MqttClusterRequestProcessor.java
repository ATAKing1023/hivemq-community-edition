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

package com.hivemq.cluster.core;

import com.alipay.sofa.jraft.Closure;
import com.hivemq.cluster.AbstractProcessor;

/**
 * MQTT集群操作处理器抽象实现
 *
 * @author ankang
 * @since 2021/11/3
 */
public abstract class MqttClusterRequestProcessor<T>
        extends AbstractProcessor<MqttClusterRequest, MqttClusterResponse, MqttClusterClosure, MqttClusterService, T> {

    protected MqttClusterRequestProcessor(final MqttClusterService raftService) {
        super(raftService);
    }

    @Override
    protected MqttClusterResponse createResponse() {
        return new MqttClusterResponse();
    }

    @Override
    protected MqttClusterClosure createClosure(
            final MqttClusterRequest request, final MqttClusterResponse response, final Closure done) {
        return new MqttClusterClosure(request, response, done);
    }
}
