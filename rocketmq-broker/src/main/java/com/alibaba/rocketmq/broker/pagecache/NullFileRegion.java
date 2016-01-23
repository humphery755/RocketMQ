package com.alibaba.rocketmq.broker.pagecache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import io.netty.channel.DefaultFileRegion;

public class NullFileRegion extends DefaultFileRegion{
	private final static FileChannel FC_ADAPTER=new NullFileChannel();
    public NullFileRegion() {
    	super(FC_ADAPTER,1,1);
    }
    
    static class NullFileChannel extends FileChannel{
    	@Override
    	public int read(ByteBuffer dst) throws IOException {
    		throw new IllegalArgumentException("UnsupportedOperationException" );
    	}

    	@Override
    	public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
    		throw new IllegalArgumentException("UnsupportedOperationException" );
    	}

    	@Override
    	public int write(ByteBuffer src) throws IOException {
    		throw new IllegalArgumentException("UnsupportedOperationException" );
    	}

    	@Override
    	public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
    		throw new IllegalArgumentException("UnsupportedOperationException" );
    	}

    	@Override
    	public long position() throws IOException {
    		throw new IllegalArgumentException("UnsupportedOperationException" );
    	}

    	@Override
    	public FileChannel position(long newPosition) throws IOException {
    		throw new IllegalArgumentException("UnsupportedOperationException" );
    	}

    	@Override
    	public long size() throws IOException {
    		throw new IllegalArgumentException("UnsupportedOperationException" );
    	}

    	@Override
    	public FileChannel truncate(long size) throws IOException {
    		throw new IllegalArgumentException("UnsupportedOperationException" );
    	}

    	@Override
    	public void force(boolean metaData) throws IOException {
    		throw new IllegalArgumentException("UnsupportedOperationException" );
    	}

    	@Override
    	public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
    		throw new IllegalArgumentException("UnsupportedOperationException" );
    	}

    	@Override
    	public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
    		throw new IllegalArgumentException("UnsupportedOperationException" );
    	}

    	@Override
    	public int read(ByteBuffer dst, long position) throws IOException {
    		throw new IllegalArgumentException("UnsupportedOperationException" );
    	}

    	@Override
    	public int write(ByteBuffer src, long position) throws IOException {
    		throw new IllegalArgumentException("UnsupportedOperationException" );
    	}

    	@Override
    	public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
    		throw new IllegalArgumentException("UnsupportedOperationException" );
    	}

    	@Override
    	public FileLock lock(long position, long size, boolean shared) throws IOException {
    		throw new IllegalArgumentException("UnsupportedOperationException" );
    	}

    	@Override
    	public FileLock tryLock(long position, long size, boolean shared) throws IOException {
    		throw new IllegalArgumentException("UnsupportedOperationException" );
    	}

    	@Override
    	protected void implCloseChannel() throws IOException {
    		throw new IllegalArgumentException("UnsupportedOperationException" );
    	}
    }

}
