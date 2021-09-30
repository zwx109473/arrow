/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.memory;

import org.apache.arrow.util.VisibleForTesting;

import io.netty.buffer.PooledByteBufAllocatorL;
import io.netty.buffer.UnsafeDirectLittleEndian;
import io.netty.util.internal.PlatformDependent;

/**
 * The default implementation of {@link MemoryChunk}. The implementation is responsible for managing when
 * memory is allocated and returned to the Netty-based PooledByteBufAllocatorL.
 */
public class NettyMemoryChunk implements MemoryChunk {

  public static final MemoryChunkAllocator ALLOCATOR = new MemoryChunkAllocator() {

    @Override
    public MemoryChunk allocate(long requestedSize) {
      return new NettyMemoryChunk(requestedSize);
    }

    @Override
    public ArrowBuf empty() {
      return EMPTY_BUFFER;
    }
  };

  /**
   * The default cut-off value for switching allocation strategies.
   * If the request size is not greater than the cut-off value, we will allocate memory by
   * {@link PooledByteBufAllocatorL} APIs,
   * otherwise, we will use {@link PlatformDependent} APIs.
   */
  public static final int DEFAULT_ALLOCATION_CUTOFF_VALUE = Integer.MAX_VALUE;

  private static final PooledByteBufAllocatorL INNER_ALLOCATOR = new PooledByteBufAllocatorL();
  static final UnsafeDirectLittleEndian EMPTY = INNER_ALLOCATOR.empty;
  static final ArrowBuf EMPTY_BUFFER = new ArrowBuf(ReferenceManager.NO_OP,
      null,
      0,
      NettyMemoryChunk.EMPTY.memoryAddress());

  private final long allocatedSize;
  private final UnsafeDirectLittleEndian nettyBuf;
  private final long allocatedAddress;

  /**
   * The cut-off value for switching allocation strategies.
   */
  private final int allocationCutOffValue;

  NettyMemoryChunk(long requestedSize, int allocationCutOffValue) {
    this.allocationCutOffValue = allocationCutOffValue;

    if (requestedSize > allocationCutOffValue) {
      this.nettyBuf = null;
      this.allocatedAddress = PlatformDependent.allocateMemory(requestedSize);
      this.allocatedSize = requestedSize;
    } else {
      this.nettyBuf = INNER_ALLOCATOR.allocate(requestedSize);
      this.allocatedAddress = nettyBuf.memoryAddress();
      this.allocatedSize = nettyBuf.capacity();
    }
  }

  NettyMemoryChunk(long requestedSize) {
    this(requestedSize, DEFAULT_ALLOCATION_CUTOFF_VALUE);
  }

  /**
   * Get the underlying netty buffer managed by this MemoryChunk.
   * @return the underlying memory chunk if the request size is not greater than the
   *   {@link NettyMemoryChunk#allocationCutOffValue}, or null otherwise.
   */
  @VisibleForTesting
  UnsafeDirectLittleEndian getNettyBuf() {
    return nettyBuf;
  }

  @Override
  public long memoryAddress() {
    return allocatedAddress;
  }

  @Override
  public void destroy() {
    if (nettyBuf == null) {
      PlatformDependent.freeMemory(allocatedAddress);
    } else {
      nettyBuf.release();
    }
  }

  /**
   * Returns the underlying memory chunk size managed.
   *
   * <p>NettyMemoryChunk rounds requested size up to the next power of two.
   */
  @Override
  public long size() {
    return allocatedSize;
  }

}
