package com.hazelcast.jet.projectx.demo;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.projectx.RedisSources;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.codec.RedisCodec;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;

public class SourceSetRead {

    private static final String REDIS_URI = "redis://localhost/";
    private static final SupplierEx<RedisCodec<String, Trade>> CODEC_SUPPLIER = TradeCodec::new;

    public static void main(String[] args) {
        try {
            JetInstance jet = Jet.newJetInstance();

            Pipeline p = Pipeline.create();
            BatchSource<ScoredValue<Trade>> source = RedisSources.sortedSetBatch("source", REDIS_URI, CODEC_SUPPLIER,
                    "trades", 0, Long.MAX_VALUE);
            p.drawFrom(source)
                    .aggregate(counting())
                    .drainTo(Sinks.logger());

            jet.newJob(p).join();
        } finally {
            Jet.shutdownAll();
        }
    }

}
