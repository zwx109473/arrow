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

import java.lang.reflect.Field;
import java.util.Optional;

/**
 * A class for choosing the default memory chunk.
 */
public class DefaultMemoryChunkAllocatorOption {

  /**
   * The environmental variable to set the default chunk allocator type.
   */
  public static final String ALLOCATION_MANAGER_TYPE_ENV_NAME = "ARROW_CHUNK_ALLOCATOR_TYPE";

  /**
   * The system property to set the default chunk allocator type.
   */
  public static final String ALLOCATION_MANAGER_TYPE_PROPERTY_NAME = "arrow.chunk.allocator.type";

  /**
   * The environmental variable to set the default MemoryChunkManager type.
   *
   * @deprecated the value has been deprecated. Use ALLOCATION_MANAGER_TYPE_ENV_NAME instead.
   */
  @Deprecated
  public static final String OBSOLETE_ALLOCATION_MANAGER_TYPE_ENV_NAME = "ARROW_ALLOCATION_MANAGER_TYPE";

  /**
   * The system property to set the default MemoryChunkManager type.
   *
   * @deprecated the value has been deprecated. Use ALLOCATION_MANAGER_TYPE_PROPERTY_NAME instead.
   */
  @Deprecated
  public static final String OBSOLETE_ALLOCATION_MANAGER_TYPE_PROPERTY_NAME = "arrow.allocation.manager.type";

  static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(DefaultMemoryChunkAllocatorOption.class);

  /**
   * The default memory chunk allocator.
   */
  private static MemoryChunkAllocator DEFAULT_CHUNK_ALLOCATOR = null;

  /**
   * The chunk allocator type.
   */
  public enum MemoryChunkAllocatorType {
    /**
     * Netty based chunk allocator.
     */
    Netty,

    /**
     * Unsafe based chunk allocator.
     */
    Unsafe,

    /**
     * Unknown type.
     */
    Unknown,
  }

  private static String getEnvWithFallbackKey(String key, String fallbackKey) {
    return Optional.of(System.getenv(key)).orElse(System.getenv(fallbackKey));
  }

  private static String getPropertyWithFallbackKey(String key, String fallbackKey) {
    return Optional.of(System.getProperty(key)).orElse(System.getenv(fallbackKey));
  }

  static MemoryChunkAllocatorType getDefaultMemoryChunkAllocatorType() {
    MemoryChunkAllocatorType ret = MemoryChunkAllocatorType.Unknown;

    try {
      String envValue = getEnvWithFallbackKey(ALLOCATION_MANAGER_TYPE_ENV_NAME,
          OBSOLETE_ALLOCATION_MANAGER_TYPE_ENV_NAME);
      ret = MemoryChunkAllocatorType.valueOf(envValue);
    } catch (IllegalArgumentException | NullPointerException e) {
      // ignore the exception, and make the chunk allocator type remain unchanged
    }

    // system property takes precedence
    try {
      String propValue = getPropertyWithFallbackKey(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
          OBSOLETE_ALLOCATION_MANAGER_TYPE_PROPERTY_NAME);
      ret = MemoryChunkAllocatorType.valueOf(propValue);
    } catch (IllegalArgumentException | NullPointerException e) {
      // ignore the exception, and make the chunk allocator type remain unchanged
    }
    return ret;
  }

  static MemoryChunkAllocator getDefaultMemoryChunkAllocator() {
    if (DEFAULT_CHUNK_ALLOCATOR != null) {
      return DEFAULT_CHUNK_ALLOCATOR;
    }
    MemoryChunkAllocatorType type = getDefaultMemoryChunkAllocatorType();
    switch (type) {
      case Netty:
        DEFAULT_CHUNK_ALLOCATOR = getNettyAllocator();
        break;
      case Unsafe:
        DEFAULT_CHUNK_ALLOCATOR = getUnsafeAllocator();
        break;
      case Unknown:
        LOGGER.info("memory chunk allocator type not specified, using netty as the default type");
        DEFAULT_CHUNK_ALLOCATOR = getAllocator(CheckAllocator.check());
        break;
      default:
        throw new IllegalStateException("Unknown memory chunk allocator type: " + type);
    }
    return DEFAULT_CHUNK_ALLOCATOR;
  }

  private static MemoryChunkAllocator getAllocator(String clazzName) {
    try {
      Field field = Class.forName(clazzName).getDeclaredField("ALLOCATOR");
      field.setAccessible(true);
      return (MemoryChunkAllocator) field.get(null);
    } catch (Exception e) {
      throw new RuntimeException("Unable to instantiate MemoryChunkAllocator for " + clazzName, e);
    }
  }

  private static MemoryChunkAllocator getUnsafeAllocator() {
    try {
      return getAllocator("org.apache.arrow.memory.UnsafeMemoryChunk");
    } catch (RuntimeException e) {
      throw new RuntimeException("Please add arrow-memory-unsafe to your classpath," +
          " No DefaultMemoryChunkAllocator found to instantiate an UnsafeMemoryChunk", e);
    }
  }

  private static MemoryChunkAllocator getNettyAllocator() {
    try {
      return getAllocator("org.apache.arrow.memory.NettyMemoryChunk");
    } catch (RuntimeException e) {
      throw new RuntimeException("Please add arrow-memory-netty to your classpath," +
          " No DefaultMemoryChunkAllocator found to instantiate an NettyMemoryChunk", e);
    }
  }
}
