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

import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.rounding.RoundingPolicy;
import org.apache.arrow.memory.rounding.SegmentRoundingPolicy;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks for allocators.
 */
public class AllocatorBenchmarks {
  private static final int BUFFER_SIZE = 1024;
  private static final int NUM_BUFFERS = 1024;
  private static final int SEGMENT_SIZE = 1024;

  /**
   * Benchmark for the default allocator.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void defaultAllocatorBenchmark() {
    final int bufferSize = BUFFER_SIZE;
    final int numBuffers = NUM_BUFFERS;

    try (RootAllocator allocator = new RootAllocator(numBuffers * bufferSize)) {
      ArrowBuf[] buffers = new ArrowBuf[numBuffers];

      for (int i = 0; i < numBuffers; i++) {
        buffers[i] = allocator.buffer(bufferSize);
      }

      for (int i = 0; i < numBuffers; i++) {
        buffers[i].close();
      }
    }
  }

  /**
   * Benchmark for allocator with segment rounding policy.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void segmentRoundingPolicyBenchmark() {
    final int bufferSize = BUFFER_SIZE;
    final int numBuffers = NUM_BUFFERS;
    final int segmentSize = SEGMENT_SIZE;

    RoundingPolicy policy = new SegmentRoundingPolicy(segmentSize);
    try (RootAllocator allocator = new RootAllocator(AllocationListener.NOOP, bufferSize * numBuffers, policy)) {
      ArrowBuf[] buffers = new ArrowBuf[numBuffers];

      for (int i = 0; i < numBuffers; i++) {
        buffers[i] = allocator.buffer(bufferSize);
      }

      for (int i = 0; i < numBuffers; i++) {
        buffers[i].close();
      }
    }
  }

  /**
   * Benchmark for memory chunk cleaner, without GC involved.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void memoryChunkCleanerBenchmarkWithoutGC() {
    final int bufferSize = BUFFER_SIZE;
    final int numBuffers = NUM_BUFFERS;

    try (RootAllocator allocator = new RootAllocator(
        BaseAllocator.configBuilder()
            .maxAllocation(bufferSize * numBuffers)
            .memoryChunkManagerFactory(MemoryChunkCleaner.newFactory())
            .listener(MemoryChunkCleaner.gcTrigger())
            .build())) {
      ArrowBuf[] buffers = new ArrowBuf[numBuffers];

      for (int i = 0; i < numBuffers; i++) {
        buffers[i] = allocator.buffer(bufferSize);
      }

      for (int i = 0; i < numBuffers; i++) {
        buffers[i].close();
      }
    }
  }

  private static final RootAllocator CLEANER_ALLOCATOR = new RootAllocator(
      BaseAllocator.configBuilder()
          // set to a larger limit to prevent GC from being performed too frequently
          .maxAllocation(BUFFER_SIZE * NUM_BUFFERS * 1000)
          .memoryChunkManagerFactory(MemoryChunkCleaner.newFactory())
          .listener(MemoryChunkCleaner.gcTrigger())
          .build());

  /**
   * Benchmark for memory chunk cleaner, with GC.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void memoryChunkCleanerBenchmarkWithGC() {
    final BufferAllocator allocator = CLEANER_ALLOCATOR;
    final int bufferSize = BUFFER_SIZE;
    final int numBuffers = NUM_BUFFERS;

    ArrowBuf[] buffers = new ArrowBuf[numBuffers];

    for (int i = 0; i < numBuffers; i++) {
      buffers[i] = allocator.buffer(bufferSize);
    }

    for (int i = 0; i < numBuffers; i++) {
      // close() is no-op for cleaner-enabled allocator. Rely on GC.
      buffers[i].close();
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(AllocatorBenchmarks.class.getSimpleName())
            .forks(1)
            .build();

    new Runner(opt).run();
  }
}
