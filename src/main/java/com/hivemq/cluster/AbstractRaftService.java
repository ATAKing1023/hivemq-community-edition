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
import com.alipay.remoting.rpc.protocol.UserProcessor;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

/**
 * Raft服务抽象实现
 *
 * @param <P> 请求类型
 * @param <R> 响应类型
 * @param <C> 回调类型
 * @author ankang
 * @since 2021/8/13
 */
@Slf4j
public abstract class AbstractRaftService<P, R extends BaseResponse, C extends AbstractClosure<P, R>>
        implements RaftService<P, R, C> {

    private final Node node;

    protected AbstractRaftService(
            final EnhancedStateMachine stateMachine, final ClusterServerManager clusterServerManager) {
        clusterServerManager.registerProcessors(getProcessors());
        this.node = clusterServerManager.createAndStartRaftNode(stateMachine);
    }

    @Override
    public void apply(final C closure) {
        if (!node.isLeader()) {
            closure.failure(RaftError.EPERM, "Not leader", getRedirect());
            return;
        }
        try {
            final Task task = new Task();
            task.setData(ByteBuffer.wrap(SerializerManager.getSerializer(SerializerManager.Hessian2)
                    .serialize(closure.getRequest())));
            task.setDone(closure);
            node.apply(task);
        } catch (final CodecException e) {
            final String errorMsg = "Failed to encode " + closure.getRequest();
            log.error(errorMsg, e);
            closure.failure(RaftError.EINTERNAL, errorMsg, null);
        }
    }

    /**
     * 获取当前服务的RPC处理类
     *
     * @return 处理类集合
     */
    protected abstract Iterable<UserProcessor<?>> getProcessors();

    private String getRedirect() {
        if (node != null) {
            final PeerId leader = node.getLeaderId();
            if (leader != null) {
                return leader.toString();
            }
        }
        return null;
    }
}
