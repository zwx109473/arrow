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
 * The underlying memory chunk of {@link MemoryChunkManager}.
 */
public interface MemoryChunk {
  /**
   * The size (in bytes) of this chunk.
   *
   * @return size of this chunk (in bytes).
   */
  long size();

  /**
   * The native address indicating this chunk.
   *
   * @return the native address of this chunk.
   */
  long memoryAddress();

  /**
   * Destroy and reclaim all memory spaces of this chunk.
   */
  void destroy();
}
