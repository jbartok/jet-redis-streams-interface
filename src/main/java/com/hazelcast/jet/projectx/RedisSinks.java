package com.hazelcast.jet.projectx;

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

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

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
            boolean flushed = LettuceFutures.awaitAll(1, TimeUnit.SECONDS, futures.toArray(new RedisFuture[0])); //todo: garbage!
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
