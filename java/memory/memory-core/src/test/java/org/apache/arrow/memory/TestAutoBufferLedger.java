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

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.util.MemoryUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import sun.misc.JavaLangRefAccess;
import sun.misc.SharedSecrets;

public class TestAutoBufferLedger {

  private static final int MAX_ALLOCATION = Integer.MAX_VALUE;
  private static RootAllocator root;

  @BeforeClass
  public static void beforeClass() {
    root = new RootAllocator(
        BaseAllocator.configBuilder()
            .maxAllocation(MAX_ALLOCATION)
            .bufferLedgerFactory(AutoBufferLedger.newFactory())
            .listener(DirectAllocationListener.INSTANCE)
            .build());
    cleanUpJvmReferences();
  }

  @Test
  public void testBufferAllocation() {
    final BufferAllocator allocator = root.newChildAllocator("TEST-CHILD", 0, MAX_ALLOCATION);
    ArrowBuf buf = allocator.buffer(2L);
    assertEquals(2L, buf.capacity());
    assertEquals(2L, allocator.getAllocatedMemory());
  }

  @Test
  public void testBufferDerivation() {
    BufferAllocator allocator = root.newChildAllocator("TEST-CHILD", 0, MAX_ALLOCATION);
    ArrowBuf buf = allocator.buffer(2);
    assertEquals(2, buf.capacity());
    assertEquals(1, buf.slice(1, 1).capacity());
    assertEquals(2, buf.slice(0, 2).capacity());
    assertEquals(2L, allocator.getAllocatedMemory());
  }

  @Test
  public void testBufferDeallocation() {
    final BufferAllocator allocator = root.newChildAllocator("TEST-CHILD", 0, MAX_ALLOCATION);
    ArrowBuf buf = allocator.buffer(2L);
    assertEquals(2L, buf.capacity());
    assertEquals(2L, allocator.getAllocatedMemory());

    // AutoBufferLedger ignores all release operations here.
    buf.getReferenceManager().release();
    assertEquals(2L, buf.capacity());
  }

  @Test
  public void testDirectMemoryReservation() {
    final BufferAllocator allocator = root.newChildAllocator("TEST-CHILD", 0, MAX_ALLOCATION);
    long prevAlloc = MemoryUtil.getCurrentDirectMemReservation();
    allocator.buffer(2L);
    long alloc = MemoryUtil.getCurrentDirectMemReservation();
    assertEquals(2L, alloc - prevAlloc);
  }

  @Test
  public void testManualGC() {
    final BufferAllocator allocator = root.newChildAllocator("TEST-CHILD", 0, MAX_ALLOCATION);
    ArrowBuf buf = allocator.buffer(2L);
    assertEquals(2L, allocator.getAllocatedMemory());
    buf = null; // make the buffer able to be discovered by garbage collector
    cleanUpJvmReferences();
    assertEquals(0L, allocator.getAllocatedMemory());
  }

  @Test
  public void testManualGCOnSharing() {
    final BufferAllocator allocator = root.newChildAllocator("TEST-CHILD", 0, MAX_ALLOCATION);
    ArrowBuf buf = allocator.buffer(2L);
    ArrowBuf sliced1 = buf.slice(1, 1);
    ArrowBuf sliced2 = buf.slice(0, 2);
    assertEquals(2L, allocator.getAllocatedMemory());
    buf = null;
    cleanUpJvmReferences();
    assertEquals(2L, allocator.getAllocatedMemory());
    sliced1 = null;
    cleanUpJvmReferences();
    assertEquals(2L, allocator.getAllocatedMemory());
    sliced2 = null;
    cleanUpJvmReferences();
    assertEquals(0L, allocator.getAllocatedMemory());
  }

  @Test
  public void testManualGCOnCrossAllocatorSharing() {
    final BufferAllocator allocator1 = root.newChildAllocator("TEST-CHILD-1", 0, MAX_ALLOCATION);
    final BufferAllocator allocator2 = root.newChildAllocator("TEST-CHILD-2", 0, MAX_ALLOCATION);
    ArrowBuf buf = allocator1.buffer(2L);
    ArrowBuf other = buf.getReferenceManager().retain(buf, allocator2);
    assertEquals(2L, allocator1.getAllocatedMemory());
    assertEquals(0L, allocator2.getAllocatedMemory());
    buf = null;
    cleanUpJvmReferences();
    assertEquals(0L, allocator1.getAllocatedMemory());
    assertEquals(2L, allocator2.getAllocatedMemory());
    other = null;
    cleanUpJvmReferences();
    assertEquals(0L, allocator1.getAllocatedMemory());
    assertEquals(0L, allocator2.getAllocatedMemory());
  }

  @Test
  public void testManualGCWithinDirectMemoryReservation() {
    final BufferAllocator allocator1 = root.newChildAllocator("TEST-CHILD-1", 0, MAX_ALLOCATION);
    final BufferAllocator allocator2 = root.newChildAllocator("TEST-CHILD-2", 0, MAX_ALLOCATION);
    long prevAlloc = MemoryUtil.getCurrentDirectMemReservation();
    ArrowBuf buffer1 = allocator1.buffer(2L);
    ArrowBuf buffer2 = buffer1.getReferenceManager().retain(buffer1, allocator2);
    long alloc1 = MemoryUtil.getCurrentDirectMemReservation();
    assertEquals(2L, alloc1 - prevAlloc);
    buffer1 = null;
    cleanUpJvmReferences();
    long alloc2 = MemoryUtil.getCurrentDirectMemReservation();
    assertEquals(2L, alloc2 - prevAlloc);
    buffer2 = null;
    cleanUpJvmReferences();
    long alloc3 = MemoryUtil.getCurrentDirectMemReservation();
    assertEquals(prevAlloc, alloc3);
  }

  @Test
  public void testFactoryClose() {
    final AutoBufferLedger.Factory factory = AutoBufferLedger.newFactory();
    final BufferAllocator alloc = new RootAllocator(
        BaseAllocator.configBuilder()
            .maxAllocation(MAX_ALLOCATION)
            .bufferLedgerFactory(factory)
            .build());
    ArrowBuf buf = alloc.buffer(2);
    assertEquals(2, buf.capacity());
    assertEquals(1, buf.slice(1, 1).capacity());
    assertEquals(2, buf.slice(0, 2).capacity());
    assertEquals(2L, alloc.getAllocatedMemory());
    factory.close();
    assertEquals(0L, alloc.getAllocatedMemory());
  }

  private static void cleanUpJvmReferences() {
    final JavaLangRefAccess jlra = SharedSecrets.getJavaLangRefAccess();
    System.gc();
    long prev = System.nanoTime();
    long sleep = 1L;
    while (true) {
      long elapsed = System.nanoTime() - prev;
      if (TimeUnit.NANOSECONDS.toMillis(elapsed) > 500L) {
        break;
      }
      if (!jlra.tryHandlePendingReference()) {
        try {
          Thread.sleep(sleep);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
        sleep = sleep << 1;
      }
    }
  }
}
