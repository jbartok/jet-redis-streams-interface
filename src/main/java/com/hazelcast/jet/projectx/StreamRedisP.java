/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.projectx;

import com.hazelcast.jet.core.AbstractProcessor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.List;

/**
 * todo add proper javadoc
 */
public class StreamRedisP extends AbstractProcessor {

    private final String connectionString;
    private final List<String> streams;
    private final String offset;

    private int processorIndex;
    private int totalParallelism;
    private boolean snapshottingEnabled;

    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> connection;

    public StreamRedisP(
            String connectionString,
            List<String> streams,
            String offset
    ) {

        this.connectionString = connectionString;
        this.streams = streams;
        this.offset = offset;
    }

    @Override
    protected void init(Context context) {
        processorIndex = context.globalProcessorIndex();
        totalParallelism = context.totalParallelism();
        snapshottingEnabled = context.snapshottingEnabled();

        redisClient = RedisClient.create("redis://localhost:6379/0");
        connection = redisClient.connect();
    }

    public boolean complete() {
        RedisCommands<String, String> sync = connection.sync();

        return false;
    }
}
