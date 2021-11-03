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

import java.util.concurrent.Future;

/**
 * 内部的精简的状态机实现
 *
 * @param <T> 操作类型
 * @author ankang
 * @since 2021/11/3
 */
public interface InternalStateMachine<T> {

    /**
     * 应用操作
     *
     * @param operation 操作
     * @return 执行的Future对象
     */
    Future<?> doApply(final T operation);
}
