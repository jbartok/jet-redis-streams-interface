package com.hazelcast.jet.projectx.demo;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.codec.Utf8StringCodec;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class TradeCodec implements RedisCodec<String, Trade> {

    private static final Charset UTF8 = Charset.forName("UTF8");

    private final StringCodec stringCodec = new Utf8StringCodec();
    private final ByteBuffer buffer = ByteBuffer.allocate(1024);

    @Override
    public String decodeKey(ByteBuffer bytes) {
        return stringCodec.decodeKey(bytes);
    }

    @Override
    public Trade decodeValue(ByteBuffer buffer) {
        long time = buffer.getLong();
        int price = buffer.getInt();
        int quantity = buffer.getInt();
        int tickerLength = buffer.getInt();
        byte[] tickerBytes = new byte[tickerLength];
        buffer.get(tickerBytes);
        String ticker = new String(tickerBytes);
        return new Trade(time, ticker, quantity, price);
    }

    @Override
    public ByteBuffer encodeKey(String key) {
        return stringCodec.encodeKey(key);
    }

    @Override
    public ByteBuffer encodeValue(Trade trade) {
        buffer.clear();
        buffer.putLong(trade.getTime());
        buffer.putInt(trade.getPrice());
        buffer.putInt(trade.getQuantity());
        byte[] tickerBytes = trade.getTicker().getBytes(UTF8);
        buffer.putInt(tickerBytes.length);
        buffer.put(tickerBytes);
        buffer.flip();
        return buffer;
    }
}
