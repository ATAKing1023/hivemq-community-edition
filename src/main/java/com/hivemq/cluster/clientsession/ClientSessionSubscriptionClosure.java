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

import com.alipay.sofa.jraft.Closure;
import com.hivemq.cluster.AbstractClosure;
import com.hivemq.cluster.clientsession.rpc.ClientSessionSubscriptionResponse;

/**
 * 客户端会话订阅操作回调
 *
 * @author ankang
 * @since 2021/8/18
 */
public class ClientSessionSubscriptionClosure
        extends AbstractClosure<ClientSessionSubscriptionOperation, ClientSessionSubscriptionResponse> {

    public ClientSessionSubscriptionClosure(
            final ClientSessionSubscriptionOperation request,
            final ClientSessionSubscriptionResponse response,
            final Closure done) {
        super(request, response, done);
    }
}
