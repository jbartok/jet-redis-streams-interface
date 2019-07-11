package com.hazelcast.jet.projectx;

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.ScoredValueStreamingChannel;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.pipeline.Sources.streamFromProcessorWithWatermarks;
import static com.hazelcast.jet.projectx.StreamRedisP.streamRedisP;
import static java.util.Collections.singletonMap;

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
        return redisStream(connectionString, streamOffsets, StringCodec::new, projectionFn);
    }

    public static StreamSource<Map<String, String>> redisStream(
            String connectionString,
            Map<String, String> streamOffsets
    ) {
        return redisStream(connectionString, streamOffsets, StreamMessage::getBody);
    }

    public static StreamSource<Map<String, String>> redisStream(
            String connectionString, String stream, String offset
    ) {
        return redisStream(connectionString, singletonMap(stream, offset), StreamMessage::getBody);
    }

    public static <K, V> BatchSource<ScoredValue<V>> sortedSetBatch(
            String name,
            String uri,
            SupplierEx<RedisCodec<K, V>> codecSupplier,
            K key,
            long rangeStart,
            long rangeEnd
    ) {
        return sortedSetBatch(name, RedisURI.create(uri), codecSupplier, key, rangeStart, rangeEnd);
    }

    public static <K, V> BatchSource<ScoredValue<V>> sortedSetBatch(
            String name,
            RedisURI uri,
            SupplierEx<RedisCodec<K, V>> codecSupplier,
            K key,
            long rangeStart,
            long rangeEnd
    ) {
        return SourceBuilder.batch(name, ctx -> new SortedSetContext<>(uri, codecSupplier.get(), key, rangeStart, rangeEnd))
                .<ScoredValue<V>>fillBufferFn(SortedSetContext::fillBuffer)
                .destroyFn(SortedSetContext::close)
                .build();
    }

    public static <K, V, T> BatchSource<T> hash(
            RedisURI uri,
            K stream,
            SupplierEx<RedisCodec<K, V>> codecSupplier,
            FunctionEx<Map.Entry<K, V>, T> mapFn
    ) {
        return SourceBuilder
                .batch("hash", context -> new HashContext<>(uri, stream, codecSupplier, mapFn))
                .<T>fillBufferFn(HashContext::fillBuffer)
                .destroyFn(HashContext::close)
                .build();
    }

    public static <T> BatchSource<T> hash(
            RedisURI uri,
            String stream,
            FunctionEx<Map.Entry<String, String>, T> mapFn
    ) {
        return hash(uri, stream, StringCodec::new, mapFn);
    }

    public static BatchSource<Map.Entry<String, String>> hash(
            RedisURI uri,
            String stream
    ) {
        return hash(uri, stream, t -> t);
    }

    private static class HashContext<K, V, T> {

        private final RedisClient redisClient;
        private final StatefulRedisConnection<K, V> connection;
        private final RedisFuture<Map<K, V>> future;
        private final FunctionEx<Map.Entry<K, V>, T> mapFn;

        public HashContext(
                RedisURI uri,
                K stream,
                SupplierEx<RedisCodec<K, V>> codecSupplier,
                FunctionEx<Map.Entry<K, V>, T> mapFn
        ) {
            redisClient = RedisClient.create(uri);
            connection = redisClient.connect(codecSupplier.get());
            future = connection.async().hgetall(stream);

            this.mapFn = mapFn;
        }

        public void fillBuffer(SourceBuilder.SourceBuffer<T> buffer) throws Exception {
            Map<K, V> map = future.get();
            map.entrySet().forEach(e -> {
                T item = mapFn.apply(e);
                if (item != null) {
                    buffer.add(item);
                }
            });
            buffer.close();
        }

        public void close() {
            connection.close();
            redisClient.shutdown();
        }
    }

    private static final class SortedSetContext<K, V> implements ScoredValueStreamingChannel<V> {

        private static final int NO_OF_ITEMS_TO_FETCH_AT_ONCE = 100;

        private final RedisClient client;
        private final StatefulRedisConnection<K, V> connection;
        private final BlockingQueue<ScoredValue<V>> queue = new ArrayBlockingQueue<>(NO_OF_ITEMS_TO_FETCH_AT_ONCE);

        private final RedisFuture<Long> commandFuture;

        private volatile InterruptedException exception;

        SortedSetContext(RedisURI uri, RedisCodec<K, V> codec, K key, long start, long stop) {
            client = RedisClient.create(uri);
            connection = client.connect(codec);

            RedisAsyncCommands<K, V> commands = connection.async();
            commandFuture = commands.zrangebyscoreWithScores(this, key, Range.create(start, stop));
        }


        void close() {
            connection.close();
            client.shutdown();
        }

        void fillBuffer(SourceBuilder.SourceBuffer<ScoredValue<V>> buffer) throws InterruptedException {
            if (exception != null) { //something went wrong on the Redis client thread
                throw exception;
            }

            for (int i = 0; i < NO_OF_ITEMS_TO_FETCH_AT_ONCE; i++) {
                ScoredValue<V> item = queue.poll(100, TimeUnit.MILLISECONDS);
                if (item == null) {
                    if (commandFuture.isDone()) {
                        buffer.close();
                    }
                    return;
                } else {
                    buffer.add(item);
                }
            }

        }

        @Override
        public void onValue(ScoredValue<V> value) {
            while (true) {
                try {
                    queue.put(value);
                    return;
                } catch (InterruptedException e) {
                    exception = e;
                }
            }
        }
    }
}
