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
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;

/**
 * 处理回调抽象实现
 *
 * @param <P> 请求类型
 * @param <R> 响应类型
 * @author ankang
 * @since 2021/8/11
 */
public abstract class AbstractClosure<P, R extends BaseResponse> implements EnhancedClosure<P, R> {

    /**
     * 请求
     */
    private final P request;
    /**
     * 响应
     */
    private final R response;
    /**
     * 网络应答callback
     */
    private final Closure done;

    protected AbstractClosure(final P request, final R response, final Closure done) {
        this.request = request;
        this.response = response;
        this.done = done;
    }

    @Override
    public void run(final Status status) {
        // 返回应答给客户端
        if (done != null) {
            done.run(status);
        }
    }

    @Override
    public void success() {
        response.setSuccess(true);
        run(Status.OK());
    }

    @Override
    public void failure(final RaftError raftError, final String errorMsg, final String redirect) {
        response.setSuccess(false);
        response.setErrorMsg(errorMsg);
        response.setRedirect(redirect);
        run(new Status(raftError, errorMsg));
    }

    @Override
    public P getRequest() {
        return request;
    }

    @Override
    public R getResponse() {
        return response;
    }
}
