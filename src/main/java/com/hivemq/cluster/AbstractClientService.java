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

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import lombok.extern.slf4j.Slf4j;

/**
 * 客户端服务抽象实现
 *
 * @author ankang
 * @since 2021/8/16
 */
@Slf4j
public abstract class AbstractClientService implements ClientService {

    private static final int REFRESH_TIMEOUT_MILLIS = 1000;
    private static final int INVOCATION_TIMEOUT_MILLIS = 5000;

    private final CliClientServiceImpl cliClientService;

    protected AbstractClientService(final ClusterServerManager clusterServerManager) {
        this.cliClientService = clusterServerManager.getCliClientService();
    }

    @Override
    public ListenableFuture<Object> invoke(final Object request) {
        final SettableFuture<Object> future = SettableFuture.create();
        try {
            final Status status =
                    RouteTable.getInstance().refreshLeader(cliClientService, getGroupId(), REFRESH_TIMEOUT_MILLIS);
            if (!status.isOk()) {
                log.warn("Failed to refresh leader: {}", status);
                throw new RaftException(EnumOutter.ErrorType.ERROR_TYPE_META, status);
            }
            final PeerId leader = RouteTable.getInstance().selectLeader(getGroupId());
            if (leader == null || leader.isEmpty()) {
                log.warn("No leader");
                throw new RaftException(EnumOutter.ErrorType.ERROR_TYPE_META, RaftError.EPERM, "No leader");
            }
            cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, (result, err) -> {
                if (err == null) {
                    future.set(result);
                } else {
                    future.setException(err);
                }
            }, INVOCATION_TIMEOUT_MILLIS);
        } catch (final Throwable e) {
            future.setException(e);
        }

        return future;
    }
}
