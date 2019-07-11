package com.hazelcast.jet.projectx;

import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RedisSinks {

    public static <K, V> Sink<ScoredValue<V>> sortedSet(
            String name,
            String uri,
            SupplierEx<RedisCodec<K, V>> codecSupplier,
            K key
    ) {
        return sortedSet(name, RedisURI.create(uri), codecSupplier, key);
    }

    public static <K, V> Sink<ScoredValue<V>> sortedSet(
            String name,
            RedisURI uri,
            SupplierEx<RedisCodec<K, V>> codecSupplier,
            K key
    ) {
        return SinkBuilder.sinkBuilder(name, ctx -> new SortedSetContext<>(uri, codecSupplier.get(), key))
                .<ScoredValue<V>>receiveFn(SortedSetContext::store)
                .flushFn(SortedSetContext::flush)
                .destroyFn(SortedSetContext::destroy)
                .build();
    }

    public static <T, K, V> Sink<T> redisStream(
            RedisURI uri,
            K stream,
            SupplierEx<RedisCodec<K, V>> codecFn,
            BiFunctionEx<K, T, Map<K, V>> mapFn
    ) {
        return SinkBuilder
                .sinkBuilder("redisStream", c -> new StreamContext<>(uri, stream, codecFn, mapFn))
                .<T>receiveFn(StreamContext::add)
                .flushFn(StreamContext::flush)
                .destroyFn(StreamContext::close)
                .build();
    }

    public static <T> Sink<T> redisStream(
            RedisURI uri,
            String stream,
            BiFunctionEx<String, T, Map<String, String>> mapFn
    ) {
        return redisStream(uri, stream, StringCodec::new, mapFn);
    }

    public static Sink<Object> redisStream(
            RedisURI uri,
            String stream
    ) {
        return redisStream(uri, stream, (s, item) -> singletonMap(s, item.toString()));
    }

    public static <K, V> Sink<Map.Entry<K, V>> hash(
            RedisURI uri,
            K stream,
            SupplierEx<RedisCodec<K, V>> codecFn
    ) {
        return SinkBuilder
                .sinkBuilder("hash", context -> new HashContext<>(uri, stream, codecFn))
                .<Map.Entry<K, V>>receiveFn(HashContext::add)
                .flushFn(HashContext::flush)
                .destroyFn(HashContext::close)
                .build();
    }

    public static Sink<Map.Entry<String, String>> hash(
            RedisURI uri,
            String stream
    ) {
        return hash(uri, stream, StringCodec::new);
    }

    private static class HashContext<K, V> {

        private final RedisClient redisClient;
        private final StatefulRedisConnection<K, V> connection;
        private final K stream;

        private final Map<K, V> map = new HashMap<>();

        public HashContext(
                RedisURI uri,
                K stream,
                SupplierEx<RedisCodec<K, V>> codecFn
        ) {
            this.stream = stream;

            redisClient = RedisClient.create(uri);
            connection = redisClient.connect(codecFn.get());
        }

        public void add(Map.Entry<K, V> item) {
            map.put(item.getKey(), item.getValue());
        }

        public void flush() {
            connection.sync().hmset(stream, map);
            map.clear();
        }

        public void close() {
            connection.close();
            redisClient.shutdown();
        }
    }

    private static class StreamContext<K, V, T> {

        private final RedisClient redisClient;
        private final StatefulRedisConnection<K, V> connection;
        private final BiFunctionEx<K, T, Map<K, V>> mapFn;
        private final K stream;
        private final List<RedisFuture<String>> futures = new ArrayList<>();

        private StreamContext(
                RedisURI uri,
                K stream,
                SupplierEx<RedisCodec<K, V>> codecFn,
                BiFunctionEx<K, T, Map<K, V>> mapFn
        ) {
            this.stream = stream;
            this.mapFn = mapFn;

            redisClient = RedisClient.create(uri);
            connection = redisClient.connect(codecFn.get());
        }

        public void add(T item) {
            RedisAsyncCommands<K, V> async = connection.async();
            RedisFuture<String> future = async.xadd(stream, mapFn.apply(stream, item));
            futures.add(future);
        }

        public void flush() {
            boolean flushed = LettuceFutures.awaitAll(10, SECONDS, futures.toArray(new RedisFuture[0]));
            if (!flushed) {
                throw new RuntimeException("Flushing failed!");
            }
            futures.clear();
        }

        public void close() {
            connection.close();
            redisClient.shutdown();
        }
    }

    private static class SortedSetContext<K, V> {

        private final RedisClient client;
        private final StatefulRedisConnection<K, V> connection;
        private final RedisAsyncCommands<K, V> commands;
        private final ArrayList<RedisFuture<Long>> futures = new ArrayList<>();
        private final K key;


        SortedSetContext(RedisURI uri, RedisCodec<K, V> codec, K key) {
            client = RedisClient.create(uri);
            connection = client.connect(codec);
            commands = connection.async();
            this.key = key;
        }

        void store(ScoredValue<V> scoredValue) {
            futures.add(commands.zadd(key, scoredValue.getScore(), scoredValue.getValue()));
        }

        void flush() {
            boolean flushed = LettuceFutures.awaitAll(1, SECONDS, futures.toArray(new RedisFuture[0])); //todo: garbage!
            if (!flushed) {
                throw new RuntimeException("Flushing failed!"); //todo: is there something better to do?
            }
            futures.clear();
        }

        void destroy() {
            connection.close();
            client.shutdown();
        }
    }

}
