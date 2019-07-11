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
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MINUTES;

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
        createJetMember();
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


    @Test
    public void benchmark() throws InterruptedException, ExecutionException, TimeoutException {
        RedisAsyncCommands<String, String> async = connection.async();

        long itemCount = 1_000_000;
        int streamCount = 1;

        for (int j = 0; j < streamCount; j++) {
            int streamIndex = j;
//            spawn(() -> {
            for (int i = 0; i < itemCount; i++) {
                async.xadd("stream-" + streamIndex, "key-" + i, "val-" + i);
            }
//            });
        }

        Map<String, String> streamOffsets = new HashMap<>();
        for (int i = 0; i < streamCount; i++) {
            streamOffsets.put("stream-" + i, "0");
        }

        Pipeline p = Pipeline.create();

        p.drawFrom(RedisSources.redisStream(redisContainer.connectionString(), streamOffsets, StreamMessage::getBody))
         .withoutTimestamps()
//         .drainTo(Sinks.list("list"));
         .drainTo(RedisSinks.redisStream(RedisURI.create(redisContainer.connectionString()), "sinkStream"));


        long begin = System.currentTimeMillis();

        instance.newJob(p);
        IListJet<Object> list = instance.getList("list");

        assertTrueEventually(() -> {
            long size = async.xlen("sinkStream").get(1, MINUTES);
//            int size = list.size();
            Assert.assertEquals(itemCount * streamCount, size);
        });
        long elapsed = System.currentTimeMillis() - begin;
        System.out.println("qwe " + elapsed);

    }

    @Test
    public void benchmark2() throws InterruptedException, ExecutionException, TimeoutException {
        RedisAsyncCommands<String, String> async = connection.async();

        long itemCount = 1_000_000;
        for (int i = 0; i < itemCount; i++) {
            async.xadd("stream", "key-" + i, "val-" + i);
        }

        long begin = System.currentTimeMillis();

        RedisCommands<String, String> sync = connection.sync();
        XReadArgs readArgs = XReadArgs.Builder.block(100).count(1000);

        int count = 0;
        String offset = "0";

        while (count < itemCount) {

            List<StreamMessage<String, String>> messages = sync.xread(readArgs, StreamOffset.from("stream", offset));
            int size = messages.size();
            count += size;
            offset = messages.get(size - 1).getId();
        }

        long elapsed = System.currentTimeMillis() - begin;
        System.out.println(elapsed);

    }


}
