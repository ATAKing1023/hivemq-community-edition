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

import com.alipay.sofa.jraft.StateMachine;

/**
 * 增强的状态机接口
 *
 * @author ankang
 * @since 2021/8/12
 */
public interface EnhancedStateMachine extends StateMachine {

    /**
     * 获取状态机所属集群分组ID
     *
     * @return 集群分组ID
     */
    String getGroupId();
}
