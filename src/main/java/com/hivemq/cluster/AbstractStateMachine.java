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

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 状态机抽象实现
 *
 * @param <P> 请求类型
 * @param <R> 响应类型
 * @param <C> 回调类型
 * @author ankang
 * @since 2021/8/13
 */
@Slf4j
public abstract class AbstractStateMachine<P, R extends BaseResponse, C extends AbstractClosure<P, R>>
        extends StateMachineAdapter implements EnhancedStateMachine {

    /**
     * Leader term
     */
    private final AtomicLong leaderTerm = new AtomicLong(-1);

    @SuppressWarnings("unchecked")
    @Override
    public void onApply(final Iterator iter) {
        // 遍历日志
        while (iter.hasNext()) {
            final C done = (C) iter.done();
            P request = null;
            // done 回调不为null，必须在应用日志后调用，如果不为 null，说明当前是leader。
            if (done != null) {
                // 当前是leader，可以直接从Closure中获取请求对象，避免反序列化
                request = done.getRequest();
            } else {
                // 其他节点应用此日志
                final ByteBuffer data = iter.getData();
                try {
                    request = SerializerManager.getSerializer(HessianCustomSerializer.INDEX)
                            .deserialize(data.array(), getRequestClass().getName());
                } catch (final CodecException e) {
                    log.warn("Fail to decode {}, data: {}", getRequestClass(), Arrays.toString(data.array()), e);
                }
            }
            if (request != null) {
                log.debug("{} at logIndex={}", request, iter.getIndex());
                try {
                    final Future<?> future = doApply(request);
                    final Object result = future == null ? null : future.get();
                    if (done != null) {
                        setResponseData(request, done.getResponse(), result);
                    }
                } catch (final Exception e) {
                    log.warn("Error applying {}", request, e);
                }
            }
            // 更新后，确保调用 done，返回应答给客户端。
            if (done != null) {
                done.success();
            }
            iter.next();
        }
    }

    @Override
    public void onLeaderStart(final long term) {
        super.onLeaderStart(term);
        leaderTerm.set(term);
    }

    @Override
    public void onLeaderStop(final Status status) {
        super.onLeaderStop(status);
        leaderTerm.set(-1L);
    }

    @Override
    public boolean isLeader() {
        return leaderTerm.get() > 0;
    }

    /**
     * 应用请求
     *
     * @param request 请求对象
     * @return 执行Future对象
     */
    protected abstract Future<?> doApply(P request);

    /**
     * 设置响应内容
     *
     * @param request  请求
     * @param response 响应
     * @param result   执行结果
     */
    protected abstract void setResponseData(P request, R response, Object result);

    /**
     * 获取状态机对应的请求类
     *
     * @return 请求类
     */
    protected abstract Class<P> getRequestClass();
}
