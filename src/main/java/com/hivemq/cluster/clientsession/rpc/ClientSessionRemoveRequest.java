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

package com.hivemq.cluster.clientsession.rpc;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import lombok.Value;

import java.io.Serializable;

/**
 * 客户端会话移除请求
 *
 * @author ankang
 * @since 2021/8/16
 */
@Value
public class ClientSessionRemoveRequest implements Serializable {

    public static final long serialVersionUID = 1L;

    @NotNull String clientId;
    boolean sendWill;
    long sessionExpiryInterval;
}
