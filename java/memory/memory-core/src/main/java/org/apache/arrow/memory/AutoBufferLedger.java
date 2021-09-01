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

import sun.misc.Cleaner;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An alternative implementation of {@link BufferLedger}. The reference is auto managed by JVM garbage collector
 * comparing to {@link LegacyBufferLedger}. Explicit calls to reference management methods such as
 * {@link #retain()} and {@link #release()} will be ignored.
 *
 * <p>
 * Note when this implementation, the accurate release time of the underlying {@link AllocationManager} may become
 * unpredictable because we are relying GC to do clean-up. As a result, it's recommended to specify very large
 * allocation limit (e.g. {@link Integer#MAX_VALUE}) to the corresponding {@link BufferAllocator} to avoid
 * unexpected allocation failures.
 * </p>
 *
 * <p>
 * Also, to let the GC be aware of these allocations when off-heap based
 * {@link AllocationManager}s are used, it's required to also add the allocated sizes to JVM direct
 * memory counter (which can be limited by specifying JVM option "-XX:MaxDirectMemorySize"). To
 * achieve this one can simply set allocator's {@link AllocationListener} to
 * {@link DirectAllocationListener}.
 * JVM should ensure that garbage collection will be performed once total reservation reached the limit.
 * </p>
 */
public class AutoBufferLedger extends BufferLedger {

  public static class Factory implements BufferLedger.Factory, AutoCloseable {
    private AutoBufferLedger tail = null;

    @Override
    public BufferLedger create(BufferAllocator allocator, AllocationManager allocationManager) {
      return new AutoBufferLedger(allocator, allocationManager, this);
    }

    private void link(AutoBufferLedger ledger) {
      synchronized (this) {
        if (ledger.next != null || ledger.prev != null) {
          throw new IllegalStateException("already linked");
        }
        if (tail == null) {
          tail = ledger;
          return;
        }
        tail.next = ledger;
        ledger.prev = tail;
        tail = ledger;
      }
    }

    private void unlink(AutoBufferLedger ledger) {
      synchronized (this) {
        if (ledger.next == ledger) {
          return;
        }
        if (ledger.prev == ledger) {
          throw new IllegalStateException();
        }
        if (ledger == tail) {
          tail = ledger.prev;
        }
        if (ledger.prev != null) {
          ledger.prev.next = ledger.next;
        }
        if (ledger.next != null) {
          ledger.next.prev = ledger.prev;
        }
        ledger.prev = ledger;
        ledger.next = ledger;
      }
    }

    @Override
    public void close() {
      synchronized (this) {
        while (tail != null) {
          final AutoBufferLedger tmp = tail.prev;
          tail.destruct();
          tail = tmp;
        }
      }
    }
  }

  public static Factory newFactory() {
    return new Factory();
  }

  private volatile long lDestructionTime = 0;
  private final AtomicInteger refCount = new AtomicInteger(0);
  private final AtomicBoolean destructed = new AtomicBoolean(false);
  private final Factory factory;

  private AutoBufferLedger prev = null;
  private AutoBufferLedger next = null;

  AutoBufferLedger(BufferAllocator allocator, AllocationManager allocationManager,
      Factory factory) {
    super(allocator, allocationManager);
    this.factory = factory;
    factory.link(this);
  }

  @Override
  protected long getDestructionTime() {
    return lDestructionTime;
  }

  @Override
  protected ReferenceManager newReferenceManager() {
    reserve0();
    final ReferenceManager rm = new BaseReferenceManager(this);
    Cleaner.create(rm, new LedgerDeallocator());
    return rm;
  }

  @Override
  public int getRefCount() {
    return refCount.get();
  }

  @Override
  protected void increment() {
    // no-op
  }

  @Override
  public boolean release() {
    return false;
  }

  @Override
  public boolean release(int decrement) {
    return false;
  }

  @Override
  public void retain() {

  }

  @Override
  public void retain(int increment) {

  }

  private void reserve0() {
    if (refCount.getAndAdd(1) == 0) {
      // no-op
    }
  }

  private void release0() {
    if (refCount.addAndGet(-1) == 0) {
      destruct();
    }
  }

  private void destruct() {
    if (!destructed.compareAndSet(false, true)) {
      return;
    }
    synchronized (getAllocationManager()) {
      final AllocationManager am = getAllocationManager();
      lDestructionTime = System.nanoTime();
      am.release(this);
    }
    factory.unlink(this);
  }

  /**
   * Release hook will be invoked by JVM cleaner.
   *
   * @see #newReferenceManager()
   */
  private class LedgerDeallocator implements Runnable {

    private LedgerDeallocator() {
    }

    @Override
    public void run() {
      release0();
    }
  }
}
