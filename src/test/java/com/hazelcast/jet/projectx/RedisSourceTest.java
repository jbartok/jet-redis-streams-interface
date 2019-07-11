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

import com.hazelcast.core.ISet;
import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.Sinks;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static org.junit.Assert.assertEquals;

public class RedisSourceTest extends JetTestSupport {

    @Rule
    public RedisContainer redisContainer = new RedisContainer();
    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> connection;
    private JetInstance instance;
    private JetInstance instanceToShutDown;

    @Before
    public void setup() {
        redisClient = redisContainer.newRedisClient();
        connection = redisClient.connect();

        instance = createJetMember();
        instanceToShutDown = createJetMember();
    }

    @After
    public void teardown() {
        connection.close();
        redisClient.shutdown();
    }

    @Test
    public void testRedisStreamSource_withSnapshotting() {

        int addCount = 60_000;
        int streamCount = 8;

        for (int i = 0; i < streamCount; i++) {
            int streamIndex = i;
            spawn(() -> fillStream("stream-" + streamIndex, addCount));
        }

        Map<String, String> streamOffsets = new HashMap<>();
        for (int i = 0; i < streamCount; i++) {
            streamOffsets.put("stream-" + i, "0");
        }

        Sink<Object> sink = SinkBuilder
                .sinkBuilder("set", c -> c.jetInstance().getHazelcastInstance().getSet("set"))
                .receiveFn(Set::add)
                .build();

        Pipeline p = Pipeline.create();
        p.drawFrom(RedisSources.redisStream(redisContainer.connectionString(), streamOffsets,
                mes -> mes.getStream() + " - " + mes.getId()))
         .withoutTimestamps()
         .drainTo(sink);


        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(EXACTLY_ONCE)
              .setSnapshotIntervalMillis(3000);
        Job job = instance.newJob(p, config);

        sleepSeconds(15);
        instanceToShutDown.shutdown();
        sleepSeconds(15);
        instanceToShutDown = createJetMember();

        Collection<Object> collection = instance.getHazelcastInstance().getSet("set");

        assertTrueEventually(() -> {
            assertEquals(addCount * streamCount, collection.size());
        });

        job.cancel();
    }

    private void fillStream(String stream, int addCount) {
        RedisCommands<String, String> commands = connection.sync();
        for (int i = 0; i < addCount; i++) {
            commands.xadd(stream, "foo-" + i, "bar" + i);
        }
        System.out.println("qwe completed adding for " + stream);
    }

}
