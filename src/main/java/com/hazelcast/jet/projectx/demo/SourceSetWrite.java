package com.hazelcast.jet.projectx.demo;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.projectx.RedisSinks;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.codec.RedisCodec;

import java.util.Optional;

public class SourceSetWrite {

    private static final String REDIS_URI = "redis://localhost/";
    private static final SupplierEx<RedisCodec<String, Trade>> CODEC_SUPPLIER = TradeCodec::new;

    public static void main(String[] args) {
        try {
            JetInstance jet = Jet.newJetInstance();

            Pipeline p = Pipeline.create();
            BatchSource<Trade> tradeSource = TradeGenerator.tradeSource(25, 5000, 200);
            Sink<ScoredValue<Trade>> sink = RedisSinks.sortedSet("source", REDIS_URI, CODEC_SUPPLIER, "trades");
            p.drawFrom(tradeSource)
                    .map(t -> ScoredValue.from(t.getTime(), Optional.of(t)))
                    .drainTo(sink);

            jet.newJob(p).join();
        } finally {
            Jet.shutdownAll();
        }
    }

}
