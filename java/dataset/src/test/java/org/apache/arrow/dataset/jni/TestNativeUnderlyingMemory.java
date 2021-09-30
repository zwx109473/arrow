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

package org.apache.arrow.dataset.jni;

import static org.junit.Assert.*;

import org.apache.arrow.memory.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNativeUnderlyingMemory {

  private RootAllocator allocator = null;

  @Before
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void tearDown() {
    allocator.close();
  }

  protected RootAllocator rootAllocator() {
    return allocator;
  }

  @Test
  public void testReservation() {
    final RootAllocator root = rootAllocator();

    final int size = 512;
    final MemoryChunk chunk = new MockUnderlyingMemory(size);
    final ArrowBuf buffer = root.buffer(chunk);

    assertEquals(size, root.getAllocatedMemory());

    buffer.close();
  }

  @Test
  public void testBufferTransfer() {
    final RootAllocator root = rootAllocator();

    BufferAllocator allocator1 = root.newChildAllocator("allocator1", 0, Long.MAX_VALUE);
    BufferAllocator allocator2 = root.newChildAllocator("allocator2", 0, Long.MAX_VALUE);
    assertEquals(0, allocator1.getAllocatedMemory());
    assertEquals(0, allocator2.getAllocatedMemory());

    final int size = 512;
    final MemoryChunk chunk = new MockUnderlyingMemory(size);
    final ArrowBuf buffer = allocator1.buffer(chunk);

    assertEquals(size, buffer.getActualMemoryConsumed());
    assertEquals(size, buffer.getPossibleMemoryConsumed());
    assertEquals(size, allocator1.getAllocatedMemory());

    final ArrowBuf transferredBuffer = buffer.getReferenceManager().retain(buffer, allocator2);
    buffer.close(); // release previous owner
    assertEquals(0, buffer.getActualMemoryConsumed());
    assertEquals(size, buffer.getPossibleMemoryConsumed());
    assertEquals(size, transferredBuffer.getActualMemoryConsumed());
    assertEquals(size, transferredBuffer.getPossibleMemoryConsumed());
    assertEquals(0, allocator1.getAllocatedMemory());
    assertEquals(size, allocator2.getAllocatedMemory());

    transferredBuffer.close();
    allocator1.close();
    allocator2.close();
  }

  /**
   * A mock class of {@link NativeUnderlyingMemory} for unit testing about size-related operations.
   */
  private static class MockUnderlyingMemory extends NativeUnderlyingMemory {

    /**
     * Constructor.
     */
    MockUnderlyingMemory(int size) {
      super(size, -1L, -1L);
    }

    @Override
    public void destroy() {
      System.out.println("Underlying memory released. Size: " + size());
    }

    @Override
    public long memoryAddress() {
      return -1L;
    }
  }
}
