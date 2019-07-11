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

import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

public class RedisSinkTest extends JetTestSupport {

    @Rule
    public RedisContainer redisContainer = new RedisContainer();
    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> connection;

    private JetInstance instance;

    @Before
    public void setup() {
        redisClient = redisContainer.newRedisClient();
        connection = redisClient.connect();

        instance = createJetMember();
    }

    @After
    public void teardown() {
        connection.close();
        redisClient.shutdown();
    }

    @Test
    public void testStream() {
        IListJet<String[]> list = instance.getList("list");
        for (int i = 0; i < 10; i++) {
            list.add(new String[]{"key-" + i, "key-" + i});
        }

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.list(list))
         .drainTo(RedisSinks.redisStream(RedisURI.create(redisContainer.connectionString()), "stream"));

        instance.newJob(p).join();

        RedisCommands<String, String> sync = connection.sync();
        List<StreamMessage<String, String>> messages = sync.xread(StreamOffset.from("stream", "0"));

        Assert.assertEquals(list.size(), messages.size());

    }
}
