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

package com.hivemq.cluster.clientqueue;

import com.google.common.primitives.ImmutableIntArray;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.mqtt.message.publish.PUBLISH;
import lombok.Data;

import java.io.Serializable;

/**
 * 客户端队列操作请求
 *
 * @author ankang
 * @since 2021/9/3
 */
@Data
public class ClientQueueOperation implements Serializable {

    private @NotNull Type type;
    private @NotNull String client;
    private @NotNull PUBLISH publish;
    private int subscriptionQos;
    private boolean shared;
    private boolean retainAsPublished;
    private @Nullable ImmutableIntArray subscriptionIdentifier;

    public enum Type {
        PUBLISH
    }
}
