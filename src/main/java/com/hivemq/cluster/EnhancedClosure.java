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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.error.RaftError;

/**
 * 增强的处理回调
 *
 * @param <P> 请求类型
 * @param <R> 响应类型
 * @author ankang
 * @since 2021/8/13
 */
public interface EnhancedClosure<P, R> extends Closure {

    /**
     * 成功响应
     */
    void success();

    /**
     * 失败响应
     *
     * @param raftError 错误类型
     * @param errorMsg  错误信息
     * @param redirect  重定向地址
     */
    void failure(RaftError raftError, String errorMsg, String redirect);

    /**
     * 获取请求对象
     *
     * @return 请求对象
     */
    P getRequest();

    /**
     * 获取响应对象
     *
     * @return 响应对象
     */
    R getResponse();
}
