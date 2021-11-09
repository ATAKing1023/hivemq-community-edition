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

import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@code Cursor} allows to access key/value pairs of a {@linkplain Store} in both successive (ascending and descending)
 * and random order. {@code Cursor} can be opened for a {@linkplain Store} in a {@linkplain Transaction}. Both key
 * ({@linkplain #getKey()}) and value ({@linkplain #getValue()}) are accessible as byte array.
 * Finally, any cursor should always be closed.
 *
 * <p>Each newly created cursor points to a "virtual" key/value pair which is prior to the first (leftmost) pair. Use
 * {@linkplain #getNext()} to move to the first and {@linkplain #getPrev()} to last (rightmost) key/value pair. You
 * can move {@code Cursor} to the last (rightmost) position from any other position using {@linkplain #getLast()}.
 */
public interface Cursor {

    /**
     * Moves the {@code Cursor} to the next key/value pair. If the {@code Cursor} is just created the method moves
     * it to the first (leftmost) key/value pair.
     *
     * <p>E.g., traversing all key/value pairs of a {@linkplain Store} in ascending order looks as follows:
     * <pre>
     * try (Cursor cursor = store.openCursor(txn)) {
     *     while (cursor.getNext()) {
     *         cursor.getKey();   // current key
     *         cursor.getValue(); // current value
     *     }
     * }
     * </pre>
     *
     * @return {@code true} if next pair exists
     */
    boolean getNext();

    /**
     * @return current key
     * @see #getValue()
     */
    @NotNull
    byte[] getKey();

    /**
     * @return current value
     * @see #getKey()
     */
    @NotNull
    byte[] getValue();

    /**
     * Deletes current key/value pair in the {@linkplain Store} and returns {@code true} if deletion succeeded.
     *
     * @return {@code true} if current key/value pair was deleted
     */
    boolean deleteCurrent();

    /**
     * Moves the {@code Cursor} to the first pair in the {@linkplain Store} whose key is equal to or greater than the
     * specified key. It returns not-null value if it succeeds or {@code null} if nothing is found, and the position of
     * the {@code Cursor} will be unchanged.
     *
     * @param key the key to search for
     * @return not-null value if it succeeds or {@code null} if nothing is found
     */
    @Nullable
    byte[] getSearchKeyRange(final @NotNull byte[] key);

}
