package com.hazelcast.jet.projectx;/*
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

import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RedisSourceTest extends JetTestSupport {

    @Rule
    public RedisContainer redisContainer = new RedisContainer();
    RedisClient redisClient;
    StatefulRedisConnection<String, String> connection;

    @Before
    public void setup() {
        redisClient = redisContainer.newRedisClient();
        connection = redisClient.connect();
    }

    @After
    public void teardown() {
        connection.close();
        redisClient.shutdown();
    }

    @Test
    public void testRedisStreamSource_withSingleStream() throws InterruptedException {
        RedisCommands<String, String> commands = connection.sync();

        int addCount = 300;

        for (int i = 0; i < addCount; i++) {
            commands.xadd("myStream", "foo-" + i, "bar-" + i);
        }

        Map<String, String> streamOffsets = new HashMap<>();
        streamOffsets.put("myStream", "0");

        Pipeline p = Pipeline.create();
        p.drawFrom(RedisSources.redisStream(redisContainer.connectionString(), streamOffsets))
         .withoutTimestamps()
         .drainTo(Sinks.list("list"));

        JetInstance instance1 = createJetMember();
        JetInstance instance2 = createJetMember();
        Job job = instance1.newJob(p);

        sleepSeconds(2);

        for (int i = 0; i < addCount; i++) {
            commands.xadd("myStream", "foo-" + i, "bar-" + i);
        }

        sleepSeconds(1);

        for (int i = 0; i < addCount; i++) {
            commands.xadd("myStream", "foo-" + i, "bar-" + i);
        }

        sleepSeconds(2);


        IListJet<Object> list = instance1.getList("list");
        assertEquals(addCount * 3, list.size());

        job.cancel();
    }

}
