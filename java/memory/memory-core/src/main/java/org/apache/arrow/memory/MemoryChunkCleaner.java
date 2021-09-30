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

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.util.VisibleForTesting;

import sun.misc.JavaLangRefAccess;
import sun.misc.SharedSecrets;

/**
 * A {@link MemoryChunkManager} implementation to clean up managed chunk automatically by leveraging JVM
 * garbage collector. This is typically used for the scenario that buffers are often created with large size
 * and being shared everywhere. For the case of extremely high TPS with small buffer sizes, completely
 * relying on this facility is not recommended since the GC overhead will be comparatively high. For that
 * case, a hybrid mode (manual + gc) is provided via {@link MemoryChunkCleaner.Mode#HYBRID}.
 *
 * <p>
 * Note when this implementation is used, an allocator-wise cleanup operation can be manually
 * triggered via {@link BufferAllocator#close()}, which internally calls {@link ChunkWeakRefLifecycles#cleanAll()}.
 * </p>
 *
 * <p>
 * Also, the built-in {@link AllocationListener}, {@link GarbageCollectorTrigger} must be set to allocator when
 * {@link MemoryChunkCleaner} is used. {@link GarbageCollectorTrigger} triggers GC automatically when the allocator
 * is full. A static instance of {@link GarbageCollectorTrigger} can be retrieved via
 * {@link MemoryChunkCleaner#gcTrigger()}. There is a detector against this so if {@link GarbageCollectorTrigger}
 * is not set, an error will be thrown when user tries to allocate buffers.
 * </p>
 */
public class MemoryChunkCleaner extends MemoryChunkManager {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MemoryChunkCleaner.class);

  private final ChunkWeakRef chunkWeakRef;
  private final Mode mode;

  private MemoryChunkCleaner(SimpleChunkWrapper wrapper, ChunkWeakRefLifecycles lifecycles,
      Mode mode) {
    super(wrapper);
    this.chunkWeakRef = ChunkWeakRef.create(wrapper, lifecycles, wrapper.getChunk(),
        getOwnerManager(), mode);
    this.mode = mode;
  }

  @Override
  void associateLedger(BufferAllocator allocator, BufferLedger ledger) {
    checkGCTriggerIsSet(allocator);
    // Do not actually associate to prevent a strong reference being held by the target allocator
  }

  private void checkGCTriggerIsSet(BufferAllocator allocator) {
    if (!(allocator.getListener() instanceof GarbageCollectorTrigger)) {
      throw new IllegalStateException("GarbageCollectorTrigger should be set as the default " +
          "AllocationListener of the parent BufferAllocator when MemoryChunkCleaner is used. " +
          "Try using MemoryChunkCleaner#gcTrigger() to get a built-in instance.");
    }
  }

  @Override
  void dissociateLedger(BufferAllocator allocator, BufferLedger ledger) {
    // Do not actually dissociate to prevent a strong being held by the target allocator
  }

  @Override
  void doRelease() {
    if (!mode.enableManualRelease) {
      // Do nothing. Rely on GC to do release.
      return;
    }
    chunkWeakRef.deallocate();
  }

  @Override
  boolean isChunkDestroyed() {
    return chunkWeakRef.destroyed.get();
  }

  /**
   * <p>
   * Create new Factory of {@link MemoryChunkCleaner}. All manual calls to {@link ArrowBuf#close()} or
   * {@link ReferenceManager#release()} will be ignored. JVM garbage collector will be responsible for
   * all cleanup work of the managed Arrow buffers as well as the underlying memory chunks.
   * </p>
   *
   * <p>
   * This is a shortcut to {@code #newFactory(Mode.GC_ONLY)}.
   * </p>
   *
   * @return the factory instance.
   */
  public static MemoryChunkManager.Factory newFactory() {
    return new Factory(Mode.GC_ONLY);
  }

  /**
   * Create new Factory of {@link MemoryChunkCleaner}. A specific {@link MemoryChunkCleaner.Mode} can be
   * specified to let Arrow decide how to handle managed chunks' lifecycles.
   *
   * @see Mode
   * @return the factory instance.
   */
  public static MemoryChunkManager.Factory newFactory(Mode mode) {
    return new Factory(mode);
  }

  /**
   * Factory. Unlike {@link MemoryChunkManager.Factory}, each factory here has its own
   * lifecycle to hold references to all managed chunks. Using {@link MemoryChunkCleaner#newFactory()}
   * to get a new instance of this.
   */
  static class Factory implements MemoryChunkManager.Factory {

    private static Map<Factory, Object> instances = new IdentityHashMap<>();

    private final Mode mode;

    private void add(Factory factory) {
      instances.put(factory, null);
    }

    private static void remove(Factory factory) {
      instances.remove(factory, null);
    }

    private final ChunkWeakRefLifecycles lifecycles = new ChunkWeakRefLifecycles();

    private Factory(Mode mode) {
      this.mode = mode;
      // Retain strong reference to Factory instance until #close() is called.
      // If we don't do this, when lifecycles gets collected by GC,
      // The chunk weak refs will then get collected without being enqueued
      // to reference queue.
      add(this);
    }

    @Override
    public MemoryChunkManager create(MemoryChunk chunk) {
      Preconditions.checkState(!(chunk instanceof SimpleChunkWrapper), "Chunk is already " +
          "managed by MemoryChunkCleaner");
      final SimpleChunkWrapper wrapper = new SimpleChunkWrapper(chunk);
      return new MemoryChunkCleaner(wrapper, lifecycles, mode);
    }

    @Override
    public void close() {
      cleanup();
      remove(this);
    }

    /**
     * Clean up all managed chunks in one shot.
     */
    public void cleanup() {
      lifecycles.cleanAll();
    }
  }

  private static class SimpleChunkWrapper implements MemoryChunk {
    private final MemoryChunk chunk;

    private SimpleChunkWrapper(MemoryChunk chunk) {
      this.chunk = chunk;
    }

    private MemoryChunk getChunk() {
      return chunk;
    }

    @Override
    public long size() {
      return chunk.size();
    }

    @Override
    public long memoryAddress() {
      return chunk.memoryAddress();
    }

    @Override
    public void destroy() {
      chunk.destroy();
    }
  }

  private static final ReferenceQueue<Object> GARBAGE_CHUNK_QUEUE = new ReferenceQueue<>();

  static {
    final CleanerThread cleanerThread = new CleanerThread("Arrow Memory Chunk Cleaner");
    cleanerThread.setPriority(Thread.MAX_PRIORITY);
    cleanerThread.setDaemon(true);
    cleanerThread.start();
  }

  private static class CleanerThread extends Thread {
    public CleanerThread(String name) {
      super(name);
    }

    @Override
    public void run() {
      try {
        while (true) {
          reclaimNext();
        }
      } catch (Throwable t) {
        System.err.println("Arrow thunk cleaner thread aborted unexpectedly!");
        t.printStackTrace();
        System.exit(1);
      }
    }

    private void reclaimNext() {
      try {
        final Reference<?> next = GARBAGE_CHUNK_QUEUE.remove();
        if (next instanceof ChunkWeakRef) {
          final ChunkWeakRef ref = (ChunkWeakRef) next;
          ref.logAsLeak();
          ref.deallocateByGC();
          return;
        }
        throw new IllegalStateException("Unreachable code");
      } catch (Exception e) {
        System.err.println("Warn: Exception thrown from chunk cleaner thread");
        e.printStackTrace();
      }
    }
  }

  private static class ChunkWeakRefLifecycles {
    private ChunkWeakRef tail = null;

    private void add(ChunkWeakRef cr) {
      synchronized (this) {
        if (cr.next != null || cr.prev != null) {
          throw new IllegalStateException("already linked");
        }
        if (tail == null) {
          tail = cr;
          return;
        }
        tail.next = cr;
        cr.prev = tail;
        tail = cr;
      }
    }

    private void remove(ChunkWeakRef cr) {
      synchronized (this) {
        if (cr.next == cr) {
          return;
        }
        if (cr.prev == cr) {
          throw new IllegalStateException();
        }
        if (cr == tail) {
          tail = cr.prev;
        }
        if (cr.prev != null) {
          cr.prev.next = cr.next;
        }
        if (cr.next != null) {
          cr.next.prev = cr.prev;
        }
        cr.prev = cr;
        cr.next = cr;
      }
    }

    public void cleanAll() {
      synchronized (this) {
        while (tail != null) {
          tail.logAsLeak();
          tail.deallocate();
        }
      }
    }

    @VisibleForTesting
    public int length() {
      int counter = 0;
      ChunkWeakRef cursor = tail;
      while (cursor != null) {
        cursor = cursor.prev;
        counter++;
      }
      return counter;
    }
  }

  /**
   * Weak reference to the actual managed chunk. All instances are linked together
   * to prevent themselves from being collected before the referent chunk gets
   * collected.
   */
  private static class ChunkWeakRef extends WeakReference<SimpleChunkWrapper> {
    private final Mode cleanerMode;
    private final ChunkWeakRefLifecycles lifecycles;
    private final MemoryChunk chunk;
    private final AtomicBoolean destroyed = new AtomicBoolean(false);
    private final long size;

    private ChunkWeakRef prev = null;
    private ChunkWeakRef next = null;

    // We avoid holding a strong reference to the owner ledger or the referent will never
    // gets collected
    private volatile BufferAllocator ownerAllocator = null;

    private ChunkWeakRef(SimpleChunkWrapper referent, ChunkWeakRefLifecycles lifecycles,
        MemoryChunk chunk, OwnerManager ownerManager, Mode cleanerMode) {
      super(referent, GARBAGE_CHUNK_QUEUE);
      this.lifecycles = lifecycles;
      this.chunk = chunk;
      this.cleanerMode = cleanerMode;
      this.lifecycles.add(this);
      this.size = chunk.size();
      ownerManager.addListener(new OwnerManager.Listener() {
        @Override
        public void onOwnerChange(BufferLedger leger) {
          ownerAllocator = leger.getAllocator();
        }
      });
    }

    private static ChunkWeakRef create(SimpleChunkWrapper referent, ChunkWeakRefLifecycles lifecycles,
        MemoryChunk chunk, OwnerManager ownerManager, Mode mode) {
      return new ChunkWeakRef(referent, lifecycles, chunk, ownerManager, mode);
    }

    private void logAsLeak() {
      if (!cleanerMode.enableLeakLog) {
        return;
      }
      if (cleanerMode.enableGCRelease) {
        logger.warn("Unclosed unused buffer detected, size: {}. Trying to destroy... ", size);
      } else {
        logger.warn("Unclosed unused buffer detected, size: {}. ", size);
      }
    }

    private void deallocateByGC() {
      if (!cleanerMode.enableGCRelease) {
        return;
      }
      deallocate();
    }

    private void deallocate() {
      if (!destroyed.compareAndSet(false, true)) {
        // do destroy only once
        return;
      }
      clear();
      if (ownerAllocator != null) {
        ownerAllocator.releaseBytes(size);
      }
      chunk.destroy();
      if (ownerAllocator != null) {
        ownerAllocator.getListener().onRelease(size);
      }
      lifecycles.remove(this);
    }

    public Mode getCleanerMode() {
      return cleanerMode;
    }
  }

  public static AllocationListener gcTrigger() {
    return GarbageCollectorTrigger.INSTANCE;
  }

  public static AllocationListener gcTrigger(AllocationListener next) {
    return new GarbageCollectorTrigger(next);
  }

  /**
   * Standard allocation listener that must be used when {@link MemoryChunkCleaner} is used.
   */
  static class GarbageCollectorTrigger implements AllocationListener {
    private static final AllocationListener INSTANCE = new GarbageCollectorTrigger();
    private static final int MAX_SLEEPS = 9;

    private final AllocationListener next;

    private GarbageCollectorTrigger(AllocationListener next) {
      this.next = next;
    }

    private GarbageCollectorTrigger() {
      this.next = AllocationListener.NOOP;
    }

    private boolean checkIfFits(Accountant accountant, long requestSize) {
      long free = accountant.getLimit() - accountant.getAllocatedMemory();
      return requestSize <= free;
    }

    @Override
    public void onPreAllocation(long size) {
      next.onPreAllocation(size);
    }

    @Override
    public void onAllocation(long size) {
      next.onAllocation(size);
    }

    @Override
    public void onRelease(long size) {
      next.onRelease(size);
    }

    @Override
    public void onChildAdded(BufferAllocator parentAllocator, BufferAllocator childAllocator) {
      next.onChildAdded(parentAllocator, childAllocator);
    }

    @Override
    public void onChildRemoved(BufferAllocator parentAllocator, BufferAllocator childAllocator) {
      next.onChildRemoved(parentAllocator, childAllocator);
    }

    @Override
    public boolean onFailedAllocation(long size, AllocationOutcome outcome) {
      return onFailed(size, outcome) || next.onFailedAllocation(size, outcome);
    }

    private boolean onFailed(final long size, final AllocationOutcome outcome) {
      Accountant accountant = outcome.getDetails()
          .orElseThrow(() -> new IllegalStateException("Allocation failed but no " +
              "details generated")).accountant;
      // Following logic is similar to Bits#tryReserveMemory except that
      // we rely on the allocator limit rather than direct memory limit.
      // Max sleep time is about 0.5 s (511 ms).
      final JavaLangRefAccess jlra = SharedSecrets.getJavaLangRefAccess();

      while (jlra.tryHandlePendingReference()) {
        if (checkIfFits(accountant, size)) {
          return true;
        }
      }

      System.gc();

      boolean interrupted = false;
      try {
        long sleepTime = 1;
        int sleeps = 0;
        while (true) {
          if (checkIfFits(accountant, size)) {
            return true;
          }
          if (sleeps >= MAX_SLEEPS) {
            break;
          }
          if (!jlra.tryHandlePendingReference()) {
            try {
              Thread.sleep(sleepTime);
              sleepTime <<= 1;
              sleeps++;
            } catch (InterruptedException e) {
              interrupted = true;
            }
          }
        }

        return false;

      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * The strategy {@link MemoryChunkCleaner} is used to handle managed chunks.
   */
  public enum Mode {
    /**
     * Rely on JVM garbage collector totally. Disable and ignore all manual release operations to
     * {@link ArrowBuf}s. Within this mode, reclaiming operations will only take place on JVM full GC
     * which can be triggered when {@link BufferAllocator} is full, or {@link BufferAllocator} is manually
     * closed.
     */
    GC_ONLY(false, true, false),

    /**
     * Similar to {@link #GC_ONLY}, but user can release the buffer manually within the release APIs. Which means,
     * users can choose to release the managed Arrow buffers by themselves using the APIs or totally rely on JVM
     * garbage collector to do the cleanup work.
     */
    HYBRID(true, true, false),

    /**
     * Similar to {@link #HYBRID}, but {@link MemoryChunkCleaner} will output logs in WARN level when
     * a unreleased chunk is detected and is prepared to be collected by GC.
     */
    HYBRID_WITH_LOG(true, true, true),

    /**
     * Do not run any GC-based cleanups. But leakage (unclosed unreferenced buffers) will be detected and
     * logged in WARN level.
     */
    LEAK_LOG_ONLY(true, false, true);

    private final boolean enableLeakLog;
    private final boolean enableManualRelease;
    private final boolean enableGCRelease;

    Mode(boolean enableManualRelease, boolean enableGCRelease, boolean enableLeakLog) {
      this.enableLeakLog = enableLeakLog;
      this.enableManualRelease = enableManualRelease;
      this.enableGCRelease = enableGCRelease;
    }

    public boolean isEnableLeakLog() {
      return enableLeakLog;
    }

    public boolean isEnableManualRelease() {
      return enableManualRelease;
    }

    public boolean isEnableGCRelease() {
      return enableGCRelease;
    }
  }
}
