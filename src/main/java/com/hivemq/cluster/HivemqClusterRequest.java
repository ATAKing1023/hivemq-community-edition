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

import com.hivemq.configuration.HivemqId;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;

/**
 * HiveMQ集群请求基类
 *
 * @author ankang
 * @since 2021/11/18
 */
@ToString
@EqualsAndHashCode
public abstract class HivemqClusterRequest implements Serializable {

    @NotNull
    private final String hivemqId;

    public HivemqClusterRequest() {
        this(HivemqId.get());
    }

    public HivemqClusterRequest(final String hivemqId) {
        this.hivemqId = hivemqId;
    }

    public String getHivemqId() {
        return hivemqId;
    }
}
