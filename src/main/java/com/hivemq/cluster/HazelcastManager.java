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

import com.hazelcast.config.*;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import com.hivemq.common.shutdown.HiveMQShutdownHook;
import com.hivemq.common.shutdown.ShutdownHooks;
import com.hivemq.configuration.entity.ClusterEntity;
import com.hivemq.configuration.service.FullConfigurationService;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Hazelcast管理器
 *
 * @author ankang
 * @since 2021/11/10
 */
@Slf4j
@Singleton
public class HazelcastManager {

    private final HazelcastInstance hazelcastInstance;

    private final ConcurrentMap<String, Set<MessageListener<Object>>> topicMessageListenerMap =
            new ConcurrentHashMap<>();

    @Inject
    public HazelcastManager(
            final @NotNull ShutdownHooks registry,
            final @NotNull FullConfigurationService fullConfigurationService) {

        final Config config = new Config();

        final NetworkConfig networkConfig = new NetworkConfig();
        final JoinConfig joinConfig = new JoinConfig();
        final TcpIpConfig tcpIpConfig = new TcpIpConfig();
        final ClusterEntity clusterConfig = fullConfigurationService.clusterConfigurationService().getClusterConfig();
        tcpIpConfig.setEnabled(true);
        tcpIpConfig.setMembers(clusterConfig.getNodeList());
        joinConfig.setTcpIpConfig(tcpIpConfig);
        networkConfig.setJoin(joinConfig);
        config.setNetworkConfig(networkConfig);

        if (clusterConfig.getNodeList().size() >= 3) {
            final CPSubsystemConfig cpSubsystemConfig = new CPSubsystemConfig();
            cpSubsystemConfig.setCPMemberCount(clusterConfig.getNodeList().size());
            config.setCPSubsystemConfig(cpSubsystemConfig);
        }

        log.info("Create hazelcast instance: {}", config);
        this.hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        registry.add(new HiveMQShutdownHook() {
            @Override
            public String name() {
                return "hazelcast";
            }

            @Override
            public void run() {
                log.info("Shutdown hazelcast instance");
                hazelcastInstance.shutdown();
            }
        });
    }

    public void registerListener(final String topic, final MessageListener<Object> messageListener) {
        final MessageListener<Object> wrappedListener = new MessageListenerWrapper<>(messageListener);
        if (hazelcastInstance == null) {
            topicMessageListenerMap.compute(topic, (key, value) -> {
                if (value == null) {
                    value = new HashSet<>();
                }
                value.add(wrappedListener);
                return value;
            });
        } else {
            hazelcastInstance.getReliableTopic(topic).addMessageListener(wrappedListener);
        }
    }

    public void publish(final String topic, final Object event) {
        log.debug("publish message: [topic={}, event={}]", topic, event);
        hazelcastInstance.getReliableTopic(topic).publish(event);
    }

    public IAtomicLong getAtomicLong(final String name) {
        return hazelcastInstance.getCPSubsystem().getAtomicLong(name);
    }

    private static class MessageListenerWrapper<E> implements MessageListener<E> {

        private final MessageListener<E> messageListener;

        public MessageListenerWrapper(final MessageListener<E> messageListener) {
            this.messageListener = messageListener;
        }

        @Override
        public void onMessage(final Message<E> message) {
            log.debug(
                    "received message: [source={}, messageObject={}, publishingMember={}, publishTime={}]",
                    message.getSource(),
                    message.getMessageObject(),
                    message.getPublishingMember(),
                    message.getPublishTime());
            this.messageListener.onMessage(message);
        }
    }
}
