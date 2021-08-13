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

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.AsyncUserProcessor;
import com.alipay.sofa.jraft.Closure;

/**
 * 请求处理类抽象实现
 *
 * @param <P> Raft请求类型
 * @param <R> 响应类型
 * @param <C> 回调类型
 * @param <S> 服务类型
 * @param <T> RPC请求类型
 * @author ankang
 * @since 2021/8/13
 */
public abstract class AbstractProcessor<P, R extends AbstractResponse, C extends EnhancedClosure<P, R>, S extends RaftService<P, R, C>, T>
        extends AsyncUserProcessor<T> {

    private final S raftService;

    protected AbstractProcessor(final S raftService) {
        this.raftService = raftService;
    }

    @Override
    public void handleRequest(final BizContext bizCtx, final AsyncContext asyncCtx, final T request) {
        final R response = createResponse();
        final C closure = createClosure(transform(request), response, status -> asyncCtx.sendResponse(response));
        raftService.apply(closure);
    }

    /**
     * 将RPC请求转换为Raft请求
     *
     * @param request RPC请求
     * @return Raft请求
     */
    protected abstract P transform(T request);

    /**
     * 创建空的响应
     *
     * @return 响应
     */
    protected abstract R createResponse();

    /**
     * 创建回调封装
     *
     * @param request  请求
     * @param response 响应
     * @param done     回调
     * @return 回调封装
     */
    protected abstract C createClosure(P request, R response, Closure done);
}
