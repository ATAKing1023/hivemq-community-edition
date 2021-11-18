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

package com.hivemq.cluster.clientsession;

import com.hivemq.cluster.HivemqClusterRequest;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.mqtt.message.connect.MqttWillPublish;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 客户端会话操作请求
 *
 * @author ankang
 * @since 2021/8/12
 */
@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ClientSessionOperation extends HivemqClusterRequest {

    private @NotNull Type type;
    private @NotNull String clientId;
    private boolean cleanStart;
    private boolean sendWill;
    private long sessionExpiryInterval;
    private @Nullable MqttWillPublish willPublish;
    private @Nullable Long queueLimit;

    public ClientSessionOperation(final String hivemqId) {
        super(hivemqId);
    }

    public enum Type {
        ADD,
        REMOVE
    }
}
