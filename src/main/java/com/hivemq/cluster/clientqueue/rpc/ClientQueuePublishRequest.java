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

package com.hivemq.cluster.clientqueue.rpc;

import com.google.common.primitives.ImmutableIntArray;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.mqtt.message.publish.PUBLISH;
import lombok.Value;

import java.io.Serializable;

/**
 * 客户端队列消息发布请求
 *
 * @author ankang
 * @since 2021/9/3
 */
@Value
public class ClientQueuePublishRequest implements Serializable {

    @NotNull String client;
    @NotNull PUBLISH publish;
    int subscriptionQos;
    boolean shared;
    boolean retainAsPublished;
    @Nullable ImmutableIntArray subscriptionIdentifier;
}
