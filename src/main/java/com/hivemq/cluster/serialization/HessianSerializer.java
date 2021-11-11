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
import com.alipay.remoting.serialization.Serializer;
import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import com.caucho.hessian.io.SerializerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Hessian2序列化类，允许未实现{@link java.io.Serializable}的数据
 */
final class HessianSerializer implements Serializer {

    private final SerializerFactory serializerFactory;

    public HessianSerializer() {
        this.serializerFactory = SerializerFactory.createDefault();
        this.serializerFactory.setAllowNonSerializable(true);
    }

    /**
     * @see Serializer#serialize(Object)
     */
    @Override
    public byte[] serialize(final Object obj) throws SerializationException {
        final ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        final Hessian2Output output = new Hessian2Output(byteArray);
        output.setSerializerFactory(serializerFactory);
        try {
            output.writeObject(obj);
            output.close();
        } catch (final IOException e) {
            throw new SerializationException("IOException occurred when Hessian serializer encode!", e);
        }

        return byteArray.toByteArray();
    }

    /**
     * @see Serializer#deserialize(byte[], String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T deserialize(final byte[] data, final String classOfT) throws DeserializationException {
        final Hessian2Input input = new Hessian2Input(new ByteArrayInputStream(data));
        input.setSerializerFactory(serializerFactory);
        final Object resultObject;
        try {
            resultObject = input.readObject();
            input.close();
        } catch (final IOException e) {
            throw new DeserializationException("IOException occurred when Hessian serializer decode!", e);
        }
        return (T) resultObject;
    }

}
