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

package com.hivemq.cluster.serialization;

import com.alipay.remoting.exception.DeserializationException;
import com.alipay.remoting.exception.SerializationException;
import com.hazelcast.nio.serialization.ByteArraySerializer;

import java.io.IOException;

/**
 * Hazelcast使用的序列化工具
 *
 * @author ankang
 * @since 2021/11/11
 */
public class HazelcastGlobalSerializer implements ByteArraySerializer<Object> {

    private final HessianSerializer serializer = new HessianSerializer();

    @Override
    public byte[] write(final Object object) throws IOException {
        try {
            return serializer.serialize(object);
        } catch (final SerializationException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Object read(final byte[] buffer) throws IOException {
        try {
            return serializer.deserialize(buffer, null);
        } catch (final DeserializationException e) {
            throw new IOException(e);
        }
    }

    @Override
    public int getTypeId() {
        return -999; // UNIQUE ID
    }
}
