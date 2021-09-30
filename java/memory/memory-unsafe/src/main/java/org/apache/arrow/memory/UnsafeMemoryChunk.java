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

import org.apache.arrow.memory.util.MemoryUtil;

/**
 * Memory chunk allocator based on unsafe API.
 */
public final class UnsafeMemoryChunk implements MemoryChunk {

  private static final ArrowBuf EMPTY = new ArrowBuf(ReferenceManager.NO_OP,
      null,
      0,
      MemoryUtil.UNSAFE.allocateMemory(0)
  );

  public static final MemoryChunkAllocator ALLOCATOR = new MemoryChunkAllocator() {

    @Override
    public MemoryChunk allocate(long requestedSize) {
      return new UnsafeMemoryChunk(requestedSize);
    }

    @Override
    public ArrowBuf empty() {
      return EMPTY;
    }
  };

  private final long allocatedSize;

  private final long allocatedAddress;

  UnsafeMemoryChunk(long requestedSize) {
    allocatedAddress = MemoryUtil.UNSAFE.allocateMemory(requestedSize);
    allocatedSize = requestedSize;
  }

  @Override
  public long size() {
    return allocatedSize;
  }

  @Override
  public long memoryAddress() {
    return allocatedAddress;
  }

  @Override
  public void destroy() {
    MemoryUtil.UNSAFE.freeMemory(allocatedAddress);
  }

}
