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

import org.apache.arrow.memory.*;

/**
 * MemoryChunkManager implementation for native allocated memory.
 */
public class NativeUnderlyingMemory implements MemoryChunk {

  private final int size;
  private final long nativeInstanceId;
  private final long address;

  /**
   * Constructor.
   *
   * @param size Size of underlying memory (in bytes)
   * @param nativeInstanceId ID of the native instance
   * @param address Address of underlying memory
   */
  NativeUnderlyingMemory(int size, long nativeInstanceId, long address) {
    this.size = size;
    this.nativeInstanceId = nativeInstanceId;
    this.address = address;
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public long memoryAddress() {
    return address;
  }

  @Override
  public void destroy() {
    JniWrapper.get().releaseBuffer(nativeInstanceId);
  }
}
