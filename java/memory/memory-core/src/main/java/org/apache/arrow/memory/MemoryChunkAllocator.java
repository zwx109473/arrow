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

/**
 * Factory interface of {@link MemoryChunk}.
 */
public interface MemoryChunkAllocator {

  /**
   * Allocate for new {@link MemoryChunk}.
   *
   * @param requestedSize the requested size of memory chunk. This could be different from the actual size of the
   *                      allocated chunk which can be accessed within {@link MemoryChunk#size()}.
   * @return the newly created {@link MemoryChunk}
   */
  MemoryChunk allocate(long requestedSize);

  /**
   * Return the empty {@link ArrowBuf} instance which internally holds a {@link MemoryChunk} within this allocator
   * type.
   *
   * @return the empty {@link ArrowBuf} instance.
   */
  ArrowBuf empty();
}
