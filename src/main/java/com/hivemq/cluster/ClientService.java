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

import com.google.common.util.concurrent.ListenableFuture;

/**
 * 客户端服务接口
 *
 * @author ankang
 * @since 2021/8/16
 */
public interface ClientService {

    /**
     * 执行远程调用
     *
     * @param request 请求
     * @return Future对象
     */
    ListenableFuture<Object> invoke(Object request);

    /**
     * 获取请求所属分组ID
     *
     * @return 集群分组ID
     */
    String getGroupId();
}
