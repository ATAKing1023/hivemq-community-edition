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

import com.alipay.remoting.CustomSerializer;
import com.alipay.remoting.CustomSerializerManager;
import com.alipay.remoting.rpc.RpcServer;
import com.alipay.remoting.rpc.protocol.UserProcessor;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.impl.BoltRpcServer;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import com.hivemq.common.shutdown.HiveMQShutdownHook;
import com.hivemq.common.shutdown.ShutdownHooks;
import com.hivemq.configuration.entity.ClusterEntity;
import com.hivemq.configuration.info.SystemInformation;
import com.hivemq.configuration.service.FullConfigurationService;
import com.hivemq.exceptions.UnrecoverableException;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * 集群服务管理器
 *
 * @author ankang
 * @since 2021/8/11
 */
@Slf4j
@Singleton
public class ClusterServerManager {

    private static final CustomSerializer CUSTOM_SERIALIZER = new HessianCustomSerializer();

    private final @NotNull ShutdownHooks registry;
    private final @NotNull SystemInformation systemInformation;
    private final PeerId peerId;
    private final Configuration initialConfiguration;

    private RpcServer rpcServer;
    private CliClientServiceImpl cliClientService;

    private final List<RaftGroupService> raftGroupServices = new ArrayList<>();

    @Inject
    public ClusterServerManager(
            final @NotNull ShutdownHooks registry,
            final @NotNull SystemInformation systemInformation,
            final @NotNull FullConfigurationService fullConfigurationService) {
        this.registry = registry;
        this.systemInformation = systemInformation;
        final ClusterEntity clusterConfig = fullConfigurationService.clusterConfigurationService().getClusterConfig();
        this.peerId = new PeerId(clusterConfig.getBindAddress(), clusterConfig.getRpcPort());
        final List<PeerId> peerIds = new ArrayList<>();
        for (final String address : clusterConfig.getNodeList()) {
            peerIds.add(new PeerId(address, clusterConfig.getRpcPort()));
        }
        this.initialConfiguration = new Configuration(peerIds);
        SerializerManager.addSerializer(HessianCustomSerializer.INDEX, HessianCustomSerializer.DEFAULT.getSerializer());
    }

    @PostConstruct
    public void postConstruct() {
        log.info("Initializing raft cluster: peerId-[{}], initialConfiguration-[{}]", peerId, initialConfiguration);
        this.rpcServer = new RpcServer(peerId.getIp(), peerId.getPort());
        RaftRpcServerFactory.addRaftRequestProcessors(new BoltRpcServer(this.rpcServer));
        try {
            this.rpcServer.startup();
            log.info("Successfully started RPC server");
        } catch (final Exception e) {
            log.error("Failed to start RPC server", e);
            throw new UnrecoverableException();
        }

        this.cliClientService = new CliClientServiceImpl();
        this.cliClientService.init(new CliOptions());

        this.registry.add(new HiveMQShutdownHook() {
            @Override
            public String name() {
                return "raft-group-service";
            }

            @Override
            public void run() {
                for (final RaftGroupService raftGroupService : raftGroupServices) {
                    log.info("Stop RaftGroupService: {}", raftGroupService.getGroupId());
                    raftGroupService.shutdown();
                }
            }
        });
    }

    /**
     * 注册RPC处理类，服务初始化时需要调用
     *
     * @param processors 处理类集合
     */
    public void registerProcessors(final Iterable<UserProcessor<?>> processors) {
        for (final UserProcessor<?> processor : processors) {
            log.info("Register user processor: {}", processor);
            // 注册业务处理器
            rpcServer.registerUserProcessor(processor);
            CustomSerializerManager.registerCustomSerializer(processor.interest(), CUSTOM_SERIALIZER);
        }
    }

    /**
     * 创建并启动Raft节点
     *
     * @param stateMachine 状态机
     * @return 节点
     */
    public Node createAndStartRaftNode(final EnhancedStateMachine stateMachine) {
        final String groupId = stateMachine.getGroupId();
        RouteTable.getInstance().updateConfiguration(groupId, initialConfiguration);
        final RaftGroupService raftGroupService =
                new RaftGroupService(groupId, peerId, createNodeOptions(stateMachine));
        raftGroupServices.add(raftGroupService);
        log.info("Start RaftGroupService: {}", raftGroupService.getGroupId());
        return raftGroupService.start(false);
    }

    /**
     * 获取命令行客户端服务
     *
     * @return 命令行客户端服务
     */
    public CliClientServiceImpl getCliClientService() {
        return cliClientService;
    }

    private NodeOptions createNodeOptions(final EnhancedStateMachine stateMachine) {
        final NodeOptions nodeOptions = new NodeOptions();
        // 设置状态机到启动参数
        nodeOptions.setFsm(stateMachine);
        // 设置存储路径
        final String dataPath = getDataPath(stateMachine.getGroupId());
        // 日志, 必须
        nodeOptions.setLogUri(dataPath + File.separator + "log");
        // 元信息, 必须
        nodeOptions.setRaftMetaUri(dataPath + File.separator + "meta");
        // snapshot, 可选, 一般都推荐
        nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot");
        // 初始节点列表
        nodeOptions.setInitialConf(initialConfiguration);
        return nodeOptions;
    }

    private synchronized String getDataPath(final String name) {
        final File dataFolder = systemInformation.getDataFolder();

        final File clusterFolder = new File(dataFolder, "cluster" + File.separator + name);
        if (!clusterFolder.exists()) {
            log.debug("Folder {} does not exist, trying to create it", clusterFolder.getAbsolutePath());
            final boolean createdDirectory = clusterFolder.mkdirs();
            if (createdDirectory) {
                log.debug("Created clusterFolder {}", dataFolder.getAbsolutePath());
            }
        }
        return clusterFolder.getAbsolutePath();
    }
}
