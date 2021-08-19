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

import com.hivemq.cluster.BaseResponse;
import com.hivemq.persistence.clientsession.callback.SubscriptionResult;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

/**
 * 客户端会话订阅操作响应
 *
 * @author ankang
 * @since 2021/8/11
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ClientSessionSubscriptionResponse extends BaseResponse implements Serializable {

    private List<SubscriptionResult> subscriptionResults;

    /**
     * 获取单条订阅的订阅结果
     *
     * @return 订阅结果
     */
    public SubscriptionResult getSubscriptionResult() {
        if (subscriptionResults == null || subscriptionResults.isEmpty()) {
            return null;
        }
        return subscriptionResults.get(0);
    }
}
