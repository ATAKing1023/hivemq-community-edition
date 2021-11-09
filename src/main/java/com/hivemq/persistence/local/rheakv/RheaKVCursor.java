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

package com.hivemq.persistence.local.rheakv;

import com.alipay.sofa.jraft.rhea.client.RheaIterator;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

import static com.hivemq.persistence.local.rheakv.RheaKVLocalPersistence.DEFAULT_BUFFER_SIZE;

/**
 * Description goes here
 *
 * @author ankang
 * @since 2021/9/15
 */
public class RheaKVCursor implements Cursor {

    private static final byte[] EMPTY_BYTES = new byte[0];

    private final RheaKVStore store;
    private RheaIterator<KVEntry> iterator;

    private List<KVEntry> history;
    private KVEntry currentEntry;

    public RheaKVCursor(final RheaKVStore store) {
        this(store, null);
    }

    public RheaKVCursor(final RheaKVStore store, final byte[] startKey) {
        this.store = store;
        this.iterator = store.iterator(startKey, null, DEFAULT_BUFFER_SIZE);
    }

    @Override
    public boolean getNext() {
        if (iterator.hasNext()) {
            currentEntry = iterator.next();
            return true;
        } else {
            return false;
        }
    }

    @NotNull
    @Override
    public byte[] getKey() {
        return currentEntry == null ? EMPTY_BYTES : currentEntry.getKey();
    }

    @NotNull
    @Override
    public byte[] getValue() {
        return currentEntry == null ? EMPTY_BYTES : currentEntry.getValue();
    }

    @Override
    public boolean deleteCurrent() {
        if (currentEntry != null) {
            return store.bDelete(currentEntry.getKey());
        }
        return false;
    }

    @Nullable
    @Override
    public byte[] getSearchKeyRange(@NotNull final byte[] key) {
        final RheaIterator<KVEntry> iterator = store.iterator(key, null, DEFAULT_BUFFER_SIZE);
        if (iterator.hasNext()) {
            this.iterator = iterator;
            this.currentEntry = iterator.next();
            return currentEntry.getValue();
        }
        return null;
    }
}
