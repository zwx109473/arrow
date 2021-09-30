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

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import sun.misc.JavaLangRefAccess;
import sun.misc.SharedSecrets;

public class TestMemoryChunkCleaner {

  private static final long MAX_ALLOCATION = Long.MAX_VALUE;
  private static RootAllocator root;
  private static MemoryChunkManager.Factory factory;

  @BeforeClass
  public static void beforeClass() {
    factory = MemoryChunkCleaner.newFactory();
    root = new RootAllocator(
        BaseAllocator.configBuilder()
            .maxAllocation(MAX_ALLOCATION)
            .memoryChunkManagerFactory(factory)
            .listener(MemoryChunkCleaner.gcTrigger())
            .build());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    ((MemoryChunkCleaner.Factory) factory).cleanup();
    closeAll(root);
  }

  private static void closeAll(BufferAllocator parent) {
    parent.getChildAllocators().forEach(TestMemoryChunkCleaner::closeAll);
    parent.close();
  }

  @Before
  public void setUp() throws Exception {
    cleanUpJvmReferences();
  }

  @Test
  public void testBufferAllocation() {
    final BufferAllocator allocator = root.newChildAllocator("TEST-CHILD", 0L, MAX_ALLOCATION);
    ArrowBuf buf = allocator.buffer(2L);
    assertEquals(2L, buf.capacity());
    assertEquals(2L, allocator.getAllocatedMemory());
  }

  @Test
  public void testBufferDerivation() {
    BufferAllocator allocator = root.newChildAllocator("TEST-CHILD", 0L, MAX_ALLOCATION);
    ArrowBuf buf = allocator.buffer(2);
    assertEquals(2L, buf.capacity());
    assertEquals(1L, buf.slice(1, 1).capacity());
    assertEquals(2L, buf.slice(0, 2).capacity());
    assertEquals(2L, allocator.getAllocatedMemory());
  }

  @Test
  public void testBufferTransfer() {
    BufferAllocator allocator1 = root.newChildAllocator("TEST-CHILD-1", 0L, MAX_ALLOCATION);
    BufferAllocator allocator2 = root.newChildAllocator("TEST-CHILD-2", 0L, MAX_ALLOCATION);
    ArrowBuf buf = allocator1.buffer(2);
    assertEquals(2L, buf.capacity());
    assertEquals(2L, buf.getActualMemoryConsumed());
    assertEquals(2L, allocator1.getAllocatedMemory());
    OwnershipTransferResult result = buf.getReferenceManager().transferOwnership(buf, allocator2);
    assertTrue(result.getAllocationFit());
    assertEquals(2L, result.getTransferredBuffer().capacity());
    assertEquals(0L, allocator1.getAllocatedMemory());
    assertEquals(2L, allocator2.getAllocatedMemory());
    assertEquals(0L, buf.getActualMemoryConsumed());
    assertEquals(2L, result.getTransferredBuffer().getActualMemoryConsumed());
  }

  @Test
  public void testManualGC() {
    final BufferAllocator allocator = root.newChildAllocator("TEST-CHILD", 0L, MAX_ALLOCATION);
    ArrowBuf buf = allocator.buffer(2L);
    assertEquals(2L, allocator.getAllocatedMemory());
    buf = null; // make the buffer able to be discovered by garbage collector
    cleanUpJvmReferences();
    assertEquals(0L, allocator.getAllocatedMemory());

    assertDoesNotThrow(allocator::close);
  }

  @Test
  public void testManualGCOnSharing() {
    final BufferAllocator allocator = root.newChildAllocator("TEST-CHILD", 0L, MAX_ALLOCATION);
    ArrowBuf buf = allocator.buffer(2L);
    ArrowBuf sliced1 = buf.slice(1, 1);
    ArrowBuf sliced2 = buf.slice(0, 2);
    buf = null;
    cleanUpJvmReferences();
    sliced1 = null;
    cleanUpJvmReferences();
    sliced2 = null;
    cleanUpJvmReferences();

    assertDoesNotThrow(allocator::close);
  }

  @Test
  public void testManualReleaseDisabled() {
    final BufferAllocator alloc = new RootAllocator(
        BaseAllocator.configBuilder()
            .maxAllocation(MAX_ALLOCATION)
            .memoryChunkManagerFactory(MemoryChunkCleaner.newFactory())
            .listener(MemoryChunkCleaner.gcTrigger())
            .build());
    ArrowBuf buffer = alloc.buffer(2L);
    assertEquals(2L, alloc.getAllocatedMemory());
    buffer.close();
    assertEquals(2L, alloc.getAllocatedMemory());
    buffer = null;
    cleanUpJvmReferences();
    assertEquals(0L, alloc.getAllocatedMemory());
  }

  @Test
  public void testManualReleaseEnabled() {
    final BufferAllocator alloc = new RootAllocator(
        BaseAllocator.configBuilder()
            .maxAllocation(MAX_ALLOCATION)
            .memoryChunkManagerFactory(MemoryChunkCleaner.newFactory(MemoryChunkCleaner.Mode.HYBRID))
            .listener(MemoryChunkCleaner.gcTrigger())
            .build());
    ArrowBuf buffer = alloc.buffer(2L);
    assertEquals(2L, alloc.getAllocatedMemory());
    buffer.close();
    assertEquals(0L, alloc.getAllocatedMemory());
    buffer = null;
    cleanUpJvmReferences();
    assertEquals(0L, alloc.getAllocatedMemory());
  }

  @Test
  public void testManualGCOnCrossAllocatorSharing() {
    final BufferAllocator allocator1 = root.newChildAllocator("TEST-CHILD-1", 0L, MAX_ALLOCATION);
    final BufferAllocator allocator2 = root.newChildAllocator("TEST-CHILD-2", 0L, MAX_ALLOCATION);
    ArrowBuf buf = allocator1.buffer(2L);
    ArrowBuf other = buf.getReferenceManager().retain(buf, allocator2);
    buf = null;
    cleanUpJvmReferences();
    other = null;
    cleanUpJvmReferences();

    assertDoesNotThrow(allocator1::close);
    assertDoesNotThrow(allocator2::close);
  }

  @Test
  public void testAllocationFailureWithoutGCTrigger() {
    // Forcibly reset child allocator's allocation listener to no-op
    final BufferAllocator allocator = root.newChildAllocator("TEST-CHILD", AllocationListener.NOOP, 0L, 2L);
    assertThrows(IllegalStateException.class, () -> allocator.buffer(2L));
  }

  @Test
  public void testAutoGC() {
    final BufferAllocator allocator = root.newChildAllocator("TEST-CHILD", 0L, 2L);
    for (int i = 0; i < 100; i++) {
      assertDoesNotThrow(() -> {
        allocator.buffer(2L);
      });
    }
  }

  @Test
  public void testOOM() {
    final BufferAllocator allocator = root.newChildAllocator("TEST-CHILD", 0L, 2L);
    ArrowBuf buf = allocator.buffer(2L);
    assertThrows(OutOfMemoryException.class, () -> allocator.buffer(2L));
  }

  @Test
  public void testAllocatorClose() {
    final MemoryChunkManager.Factory factory = MemoryChunkCleaner.newFactory();
    final BufferAllocator alloc = new RootAllocator(
        BaseAllocator.configBuilder()
            .maxAllocation(MAX_ALLOCATION)
            .memoryChunkManagerFactory(factory)
            .listener(MemoryChunkCleaner.gcTrigger())
            .build());
    ArrowBuf buf = alloc.buffer(2);
    assertEquals(2, buf.capacity());
    assertEquals(1, buf.slice(1, 1).capacity());
    assertEquals(2, buf.slice(0, 2).capacity());
    assertEquals(2L, alloc.getAllocatedMemory());
    alloc.close();
    assertEquals(0L, alloc.getAllocatedMemory());

    assertDoesNotThrow(alloc::close);
  }

  @Test
  public void testAllocatorCloseWithMultipleBuffersCreated() { // create more buffers
    final MemoryChunkManager.Factory factory = MemoryChunkCleaner.newFactory();
    final BufferAllocator alloc = new RootAllocator(
        BaseAllocator.configBuilder()
            .maxAllocation(MAX_ALLOCATION)
            .memoryChunkManagerFactory(factory)
            .listener(MemoryChunkCleaner.gcTrigger())
            .build());
    alloc.buffer(2);
    alloc.buffer(2);
    alloc.buffer(2);
    alloc.buffer(2);
    alloc.buffer(2);
    assertEquals(10L, alloc.getAllocatedMemory());
    alloc.close();
    assertEquals(0L, alloc.getAllocatedMemory());

    assertDoesNotThrow(alloc::close);
  }

  @Test
  public void testBufferAccessAfterAllocatorClosed() {
    final MemoryChunkManager.Factory factory = MemoryChunkCleaner.newFactory();
    final BufferAllocator alloc = new RootAllocator(
        BaseAllocator.configBuilder()
            .maxAllocation(MAX_ALLOCATION)
            .memoryChunkManagerFactory(factory)
            .listener(MemoryChunkCleaner.gcTrigger())
            .build());
    ArrowBuf buf = alloc.buffer(2);
    buf.setBytes(0, new byte[]{0, 0});
    assertTrue(buf.isOpen());
    assertEquals(0, buf.getByte(0));
    assertEquals(0, buf.getByte(1));
    assertDoesNotThrow(alloc::close);
    assertFalse(buf.isOpen());
    assertThrows(IllegalStateException.class, () -> buf.getByte(0));
  }

  @Test
  public void testAllocatorGCedBeforeBuffers() {
    // allocator gets garbage collected before buffers
    BufferAllocator alloc = new RootAllocator(
        BaseAllocator.configBuilder()
            .maxAllocation(MAX_ALLOCATION)
            .memoryChunkManagerFactory(MemoryChunkCleaner.newFactory())
            .listener(MemoryChunkCleaner.gcTrigger())
            .build());
    ArrowBuf buffer = alloc.buffer(2);
    alloc = null;
    cleanUpJvmReferences();
    buffer = null;
    cleanUpJvmReferences();
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
