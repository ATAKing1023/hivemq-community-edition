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

import com.hazelcast.cluster.Member;
import com.hazelcast.config.*;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import com.hivemq.cluster.event.HazelcastTopic;
import com.hivemq.cluster.serialization.HazelcastGlobalSerializer;
import com.hivemq.common.shutdown.HiveMQShutdownHook;
import com.hivemq.common.shutdown.ShutdownHooks;
import com.hivemq.configuration.entity.ClusterEntity;
import com.hivemq.configuration.service.FullConfigurationService;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;
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

    private final Config config;

    private final ShutdownHooks registry;

    private HazelcastInstance hazelcastInstance;

    private final ConcurrentMap<HazelcastTopic, Set<MessageListener<Object>>> topicMessageListenerMap =
            new ConcurrentHashMap<>();

    @Inject
    public HazelcastManager(
            final @NotNull ShutdownHooks registry, final @NotNull FullConfigurationService fullConfigurationService) {
        this.config = createConfig(fullConfigurationService.clusterConfigurationService().getClusterConfig());
        this.registry = registry;
    }

    @PostConstruct
    public void postConstruct() {
        log.info("Create hazelcast instance: {}", config);
        hazelcastInstance = Hazelcast.getOrCreateHazelcastInstance(config);
        // pre-init raft group for performance
        for (final HazelcastTopic topic : HazelcastTopic.values()) {
            hazelcastInstance.getReliableTopic(topic.name());
        }
        if (config.getCPSubsystemConfig().getCPMemberCount() > 0) {
            for (final RaftGroupId groupId : RaftGroupId.values()) {
                hazelcastInstance.getCPSubsystem().getCPSubsystemManagementService().getCPGroup(groupId.name());
            }
        }
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

    private Config createConfig(final ClusterEntity clusterConfig) {
        final Config config = new Config("mqtt-hazelcast");

        final NetworkConfig networkConfig = new NetworkConfig();
        final JoinConfig joinConfig = new JoinConfig();
        final TcpIpConfig tcpIpConfig = new TcpIpConfig();
        tcpIpConfig.setEnabled(true);
        tcpIpConfig.setMembers(clusterConfig.getNodeList());
        joinConfig.setTcpIpConfig(tcpIpConfig);
        networkConfig.setJoin(joinConfig);
        config.setNetworkConfig(networkConfig);

        if (clusterConfig.getNodeList().size() >= CPSubsystemConfig.MIN_GROUP_SIZE) {
            final CPSubsystemConfig cpSubsystemConfig = new CPSubsystemConfig();
            cpSubsystemConfig.setCPMemberCount(clusterConfig.getNodeList().size());
            cpSubsystemConfig.setGroupSize(CPSubsystemConfig.MIN_GROUP_SIZE);
            config.setCPSubsystemConfig(cpSubsystemConfig);
        }

        final SerializationConfig serializationConfig = new SerializationConfig();
        final GlobalSerializerConfig globalSerializerConfig = new GlobalSerializerConfig();
        globalSerializerConfig.setImplementation(new HazelcastGlobalSerializer());
        globalSerializerConfig.setOverrideJavaSerialization(true);
        serializationConfig.setGlobalSerializerConfig(globalSerializerConfig);
        config.setSerializationConfig(serializationConfig);
        return config;
    }

    public void registerListener(final HazelcastTopic topic, final MessageListener<Object> messageListener) {
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
            hazelcastInstance.getReliableTopic(topic.name()).addMessageListener(wrappedListener);
        }
    }

    public void publish(final HazelcastTopic topic, final Object event) {
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
            final Member publishingMember = message.getPublishingMember();
            if (publishingMember.localMember()) {
                logMessage("ignore local message: ", message);
            } else {
                logMessage("received message: ", message);
                this.messageListener.onMessage(message);
            }
        }

        private void logMessage(final String content, final Message<?> message) {
            log.debug(
                    content + "[source={}, messageObject={}, publishingMember={}, publishTime={}]",
                    message.getSource(),
                    message.getMessageObject(),
                    message.getPublishingMember(),
                    message.getPublishTime());
        }
    }
}
