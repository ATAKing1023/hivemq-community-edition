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

import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.hivemq.persistence.LocalPersistence;
import lombok.extern.slf4j.Slf4j;

/**
 * 基于本地存储实现的快照支持
 *
 * @param <T> 本地存储类型
 * @author ankang
 * @since 2021/8/18
 */
@Slf4j
public abstract class LocalPersistenceSnapshotSupport<T extends LocalPersistence> implements SnapshotSupport {

    private final T localPersistence;
    private final T snapshotPersistence;

    protected LocalPersistenceSnapshotSupport(final T localPersistence, final T snapshotPersistence) {
        this.localPersistence = localPersistence;
        this.snapshotPersistence = snapshotPersistence;
        if (localPersistence == snapshotPersistence) {
            log.warn("The snapshot persistence {} is exactly same as local persistence", snapshotPersistence);
        }
    }

    @Override
    public void doSnapshotSave(final SnapshotWriter writer) throws Exception {
        transfer(localPersistence, snapshotPersistence);
    }

    @Override
    public void doSnapshotLoad(final SnapshotReader reader) throws Exception {
        transfer(snapshotPersistence, localPersistence);
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
