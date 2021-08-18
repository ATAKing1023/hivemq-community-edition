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
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.hivemq.persistence.LocalPersistence;
import lombok.extern.slf4j.Slf4j;

/**
 * 基于本地存储实现快照的状态机框架实现
 *
 * @param <T> 本地存储类型
 * @param <P> 请求类型
 * @param <R> 响应类型
 * @param <C> 回调类型
 * @author ankang
 * @since 2021/8/18
 */
@Slf4j
public abstract class LocalPersistenceBasedStateMachine<T extends LocalPersistence, P, R extends AbstractResponse, C extends AbstractClosure<P, R>>
        extends AbstractStateMachine<P, R, C> {

    private final T localPersistence;
    private final T snapshotPersistence;

    protected LocalPersistenceBasedStateMachine(final T localPersistence, final T snapshotPersistence) {
        this.localPersistence = localPersistence;
        this.snapshotPersistence = snapshotPersistence;
        if (localPersistence == snapshotPersistence) {
            log.warn("The snapshot persistence {} is exactly same as local persistence", snapshotPersistence);
        }
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        log.info("Saving snapshot for {}", getGroupId());
        try {
            transfer(localPersistence, snapshotPersistence);
            done.run(Status.OK());
        } catch (final Exception e) {
            done.run(new Status(RaftError.EIO, "Error saving snapshot %s", e.getMessage()));
        }
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        if (isLeader()) {
            log.warn("Leader is not supposed to load snapshot");
            return false;
        }
        log.info("Loading snapshot for {}", getGroupId());
        try {
            transfer(snapshotPersistence, localPersistence);
            return true;
        } catch (final Exception e) {
            log.error("Error loading snapshot", e);
        }
        return false;
    }

    /**
     * 在存储间进行数据迁移
     *
     * @param fromPersistence 源存储
     * @param toPersistence   目标存储
     * @throws Exception 过程中发生的异常
     */
    protected abstract void transfer(T fromPersistence, T toPersistence) throws Exception;
}
