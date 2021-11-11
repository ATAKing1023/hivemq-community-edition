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

import com.alipay.remoting.DefaultCustomSerializer;
import com.alipay.remoting.InvokeContext;
import com.alipay.remoting.exception.DeserializationException;
import com.alipay.remoting.exception.SerializationException;
import com.alipay.remoting.rpc.RequestCommand;
import com.alipay.remoting.rpc.ResponseCommand;
import com.alipay.remoting.rpc.protocol.RpcRequestCommand;
import com.alipay.remoting.rpc.protocol.RpcResponseCommand;

/**
 * 自定义的RPC请求响应Hessian序列化类
 *
 * @author ankang
 * @since 2021/9/6
 */
public class HessianCustomSerializer extends DefaultCustomSerializer {

    /**
     * 在{@link com.alipay.remoting.serialization.SerializerManager}中的位置
     */
    public static final byte INDEX = 0;

    public static final HessianCustomSerializer DEFAULT = new HessianCustomSerializer();

    private final HessianSerializer serializer;

    public HessianCustomSerializer() {
        this.serializer = new HessianSerializer();
    }

    /**
     * 获取序列化工具实例
     *
     * @return 序列化工具实例
     */
    public HessianSerializer getSerializer() {
        return serializer;
    }

    @Override
    public <T extends RequestCommand> boolean serializeContent(final T request, final InvokeContext invokeContext)
            throws SerializationException {
        try {
            final RpcRequestCommand cmd = (RpcRequestCommand) request;
            request.setContent(serializer.serialize(cmd.getRequestObject()));
        } catch (final SerializationException e) {
            throw e;
        } catch (final Exception e) {
            throw new SerializationException("SerializationException", e);
        }
        return true;
    }

    @Override
    public <T extends ResponseCommand> boolean serializeContent(final T response) throws SerializationException {
        try {
            final RpcResponseCommand cmd = (RpcResponseCommand) response;
            response.setContent(serializer.serialize(cmd.getResponseObject()));
        } catch (final SerializationException e) {
            throw e;
        } catch (final Exception e) {
            throw new SerializationException("SerializationException", e);
        }
        return true;
    }

    @Override
    public <T extends RequestCommand> boolean deserializeContent(final T request) throws DeserializationException {
        try {
            final RpcRequestCommand cmd = (RpcRequestCommand) request;
            cmd.setRequestObject(serializer.deserialize(cmd.getContent(), cmd.getRequestClass()));
        } catch (final DeserializationException e) {
            throw e;
        } catch (final Exception e) {
            throw new DeserializationException("", e);
        }
        return true;
    }

    @Override
    public <T extends ResponseCommand> boolean deserializeContent(
            final T response, final InvokeContext invokeContext) throws DeserializationException {
        try {
            final RpcResponseCommand cmd = (RpcResponseCommand) response;
            cmd.setResponseObject(serializer.deserialize(cmd.getContent(), cmd.getResponseClass()));
        } catch (final DeserializationException e) {
            throw e;
        } catch (final Exception e) {
            throw new DeserializationException("", e);
        }
        return true;
    }

}
