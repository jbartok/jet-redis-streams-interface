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

import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.pipeline.StreamSource;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

import java.util.Map;

import static com.hazelcast.jet.pipeline.Sources.streamFromProcessorWithWatermarks;
import static com.hazelcast.jet.projectx.StreamRedisP.streamRedisP;

public class RedisSources {

    public static <K, V, T> StreamSource<T> redisStream(
            String connectionString,
            Map<K, String> streamOffsets,
            SupplierEx<RedisCodec<K, V>> codecFn,
            FunctionEx<? super StreamMessage<K, V>, ? extends T> projectionFn
    ) {
        return streamFromProcessorWithWatermarks("redisStreamSource",
                w -> streamRedisP(connectionString, streamOffsets, w, codecFn, projectionFn), false);
    }

    public static <T> StreamSource<T> redisStream(
            String connectionString,
            Map<String, String> streamOffsets,
            FunctionEx<? super StreamMessage<String, String>, ? extends T> projectionFn
    ) {
        return streamFromProcessorWithWatermarks("redisStreamSource",
                w -> streamRedisP(connectionString, streamOffsets, w, StringCodec::new, projectionFn), false);
    }

    public static StreamSource<Map<String, String>> redisStream(
            String connectionString,
            Map<String, String> streamOffsets
    ) {
        return streamFromProcessorWithWatermarks("redisStreamSource",
                w -> streamRedisP(connectionString, streamOffsets, w, StringCodec::new, StreamMessage::getBody), false);
    }

}
