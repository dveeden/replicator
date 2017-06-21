package com.booking.replication.binlog.common;

/**
 * Created by bosko on 6/21/17.
 */
public class ExtractedColumnBytes {

    private final int type;
    private final byte[] bytes;

    public ExtractedColumnBytes(int type, byte[] bytes) {
        this.type = type;
        this.bytes = bytes;
    }

    public int getType() { return  type; }

    public byte[] getBytes() { return  bytes; }
}
