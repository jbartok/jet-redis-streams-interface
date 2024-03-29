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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.nio.Address;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.core.EventTimeMapper.NO_NATIVE_TIME;
import static com.hazelcast.util.CollectionUtil.isEmpty;

/**
 * todo add proper javadoc
 */
public class StreamRedisP<K, V, T> extends AbstractProcessor {

    private static final Duration POLL_TIMEOUT_MS = Duration.ofMillis(50);
    private static final int BATCH_COUNT = 1_000;

    private final String connectionString;
    private final Map<K, String> streamOffsets;
    private final SupplierEx<RedisCodec<K, V>> codecFn;
    private final EventTimeMapper<? super T> eventTimeMapper;
    private final FunctionEx<? super StreamMessage<K, V>, ? extends T> projectionFn;

    private RedisClient redisClient;
    private StatefulRedisConnection<K, V> connection;
    private XReadArgs xReadArgs;

    private Traverser<Object> traverser = Traversers.empty();
    private Traverser<Map.Entry<BroadcastKey<K>, Object[]>> snapshotTraverser;

    public StreamRedisP(
            String connectionString,
            Map<K, String> streamOffsets,
            EventTimePolicy<? super T> eventTimePolicy,
            SupplierEx<RedisCodec<K, V>> codecFn,
            FunctionEx<? super StreamMessage<K, V>, ? extends T> projectionFn
    ) {

        this.connectionString = connectionString;
        this.streamOffsets = streamOffsets;
        this.codecFn = codecFn;
        this.projectionFn = projectionFn;

        this.eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
    }

    @Override
    protected void init(Context context) {
        redisClient = RedisClient.create(connectionString);
        connection = redisClient.connect(codecFn.get());

        xReadArgs = XReadArgs.Builder.block(POLL_TIMEOUT_MS).count(BATCH_COUNT);
        eventTimeMapper.addPartitions(streamOffsets.size());
    }

    public boolean complete() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        RedisCommands<K, V> sync = connection.sync();
        StreamOffset[] streamOffsetArray = streamOffsets
                .entrySet()
                .stream()
                .map(entry -> StreamOffset.from(entry.getKey(), entry.getValue()))
                .toArray(StreamOffset[]::new);
        List<StreamMessage<K, V>> messages = sync.xread(xReadArgs, streamOffsetArray);

        traverser = isEmpty(messages) ? eventTimeMapper.flatMapIdle() :
                traverseIterable(messages)
                        .flatMap(message -> {
                            streamOffsets.put(message.getStream(), message.getId());
                            T projectedMessage = projectionFn.apply(message);
                            if (projectedMessage == null) {
                                return Traversers.empty();
                            }
                            return eventTimeMapper.flatMapEvent(projectedMessage, 0, NO_NATIVE_TIME);
                        });

        emitFromTraverser(traverser);

        return false;
    }

    @Override
    public void close() {
        if (connection != null) {
            connection.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    @Override
    public boolean saveToSnapshot() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        if (snapshotTraverser == null) {
            Stream<Map.Entry<BroadcastKey<K>, Object[]>> snapshotStream = streamOffsets
                    .entrySet()
                    .stream()
                    .map(entry -> {
                        long watermark = eventTimeMapper.getWatermark(0);
                        return entry(broadcastKey(entry.getKey()), new Object[]{entry.getValue(), watermark});
                    });
            snapshotTraverser = traverseStream(snapshotStream)
                    .onFirstNull(() -> {
                        snapshotTraverser = null;
                        if (getLogger().isFineEnabled()) {
                            getLogger().fine("Finished saving snapshot. Saved offsets: " + streamOffsets);
                        }
                    });
        }

        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    public void restoreFromSnapshot(Object key, Object value) {
        K stream = ((BroadcastKey<K>) key).key();
        Object[] objects = (Object[]) value;
        String offset = (String) objects[0];
        long watermark = (long) objects[1];

        String oldOffset = streamOffsets.get(stream);
        if (oldOffset == null) {
            getLogger().warning("Offset for stream '" + stream
                    + "' is present in snapshot, but the stream is not supposed to be read");
            return;
        }
        streamOffsets.put(stream, offset);
        eventTimeMapper.restoreWatermark(0, watermark);
    }

    @Override
    public boolean finishSnapshotRestore() {
        if (getLogger().isFineEnabled()) {
            getLogger().fine("Finished restoring snapshot. Restored offsets: " + streamOffsets);
        }
        return true;
    }

    static <K, V, T> ProcessorMetaSupplier streamRedisP(
            String connectionString,
            Map<K, String> streamOffsets,
            EventTimePolicy<? super T> eventTimePolicy,
            SupplierEx<RedisCodec<K, V>> codecFn,
            FunctionEx<? super StreamMessage<K, V>, ? extends T> projectionFn
    ) {
        return new MetaSupplier<>(connectionString, streamOffsets, eventTimePolicy, codecFn, projectionFn);
    }

    static class MetaSupplier<K, V, T> implements ProcessorMetaSupplier {

        private final String connectionString;
        private final Map<K, String> streamOffsets;
        private final EventTimePolicy<? super T> eventTimePolicy;
        private final SupplierEx<RedisCodec<K, V>> codecFn;
        private final FunctionEx<? super StreamMessage<K, V>, ? extends T> projectionFn;

        MetaSupplier(
                String connectionString,
                Map<K, String> streamOffsets,
                EventTimePolicy<? super T> eventTimePolicy,
                SupplierEx<RedisCodec<K, V>> codecFn,
                FunctionEx<? super StreamMessage<K, V>, ? extends T> projectionFn
        ) {
            this.connectionString = connectionString;
            this.streamOffsets = streamOffsets;
            this.eventTimePolicy = eventTimePolicy;
            this.codecFn = codecFn;
            this.projectionFn = projectionFn;
        }

        @Override
        public int preferredLocalParallelism() {
            return 2;
        }

        @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(List<Address> addresses) {
            int size = addresses.size();
            Object[] array = streamOffsets.keySet().toArray();
            Arrays.sort(array);

            Map<Address, Map<K, String>> addressAssignment = new HashMap<>();
            addresses.forEach(address -> addressAssignment.put(address, new HashMap<>()));
            for (int i = 0; i < array.length; i++) {
                K stream = (K) array[i];
                int addressIndex = i % size;
                Address address = addresses.get(addressIndex);
                addressAssignment.get(address).put(stream, streamOffsets.get(stream));
            }
            return address -> new ProcSupplier<>(connectionString, addressAssignment.get(address),
                    eventTimePolicy, codecFn, projectionFn);
        }
    }

    static class ProcSupplier<K, V, T> implements ProcessorSupplier {

        private final String connectionString;
        private final Map<K, String> streamOffsets;
        private final EventTimePolicy<? super T> eventTimePolicy;
        private final SupplierEx<RedisCodec<K, V>> codecFn;
        private final FunctionEx<? super StreamMessage<K, V>, ? extends T> projectionFn;

        ProcSupplier(
                String connectionString,
                Map<K, String> streamOffsets,
                EventTimePolicy<? super T> eventTimePolicy,
                SupplierEx<RedisCodec<K, V>> codecFn,
                FunctionEx<? super StreamMessage<K, V>, ? extends T> projectionFn
        ) {
            this.connectionString = connectionString;
            this.streamOffsets = streamOffsets;
            this.eventTimePolicy = eventTimePolicy;
            this.codecFn = codecFn;
            this.projectionFn = projectionFn;
        }

        @Override
        public Collection<? extends Processor> get(int count) {
            Object[] array = streamOffsets.keySet().toArray();
            Arrays.sort(array);

            Map<Integer, Map<K, String>> assignment = new HashMap<>();
            IntStream.range(0, count).forEach(address -> assignment.put(address, new HashMap<>()));
            for (int i = 0; i < array.length; i++) {
                K stream = (K) array[i];
                int addressIndex = i % count;
                assignment.get(addressIndex).put(stream, streamOffsets.get(stream));
            }
            ArrayList<Processor> list = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                Map<K, String> assignedStreamOffsets = assignment.get(i);
                if (assignedStreamOffsets.isEmpty()) {
                    list.add(Processors.noopP().get());
                } else {
                    list.add(new StreamRedisP<>(connectionString, assignedStreamOffsets, eventTimePolicy,
                            codecFn, projectionFn));
                }
            }
            return list;
        }
    }
}
