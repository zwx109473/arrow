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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.util.Preconditions;

/**
 * Legacy implementation of {@link BufferLedger}. The reference count should be manually managed by
 * explicitly invoking methods {@link #retain()} and {@link #release()}, etc.
 */
public class LegacyBufferLedger extends BufferLedger {

  public static final Factory FACTORY = new Factory() {
    @Override
    public BufferLedger create(BufferAllocator allocator, AllocationManager allocationManager) {
      return new LegacyBufferLedger(allocator, allocationManager);
    }
  };

  private final AtomicInteger bufRefCnt = new AtomicInteger(0); // start at zero so we can
  private volatile long lDestructionTime = 0;

  LegacyBufferLedger(BufferAllocator allocator, AllocationManager allocationManager) {
    super(allocator, allocationManager);
  }

  /**
   * Get this ledger's reference count.
   * @return reference count
   */
  @Override
  public int getRefCount() {
    return bufRefCnt.get();
  }


  @Override
  protected void increment() {
    bufRefCnt.incrementAndGet();
  }

  @Override
  protected long getDestructionTime() {
    return lDestructionTime;
  }

  @Override
  protected ReferenceManager newReferenceManager() {
    return new BaseReferenceManager(this);
  }

  /**
   * Decrement the ledger's reference count for the associated underlying
   * memory chunk. If the reference count drops to 0, it implies that
   * no ArrowBufs managed by this reference manager need access to the memory
   * chunk. In that case, the ledger should inform the allocation manager
   * about releasing its ownership for the chunk. Whether or not the memory
   * chunk will be released is something that {@link AllocationManager} will
   * decide since tracks the usage of memory chunk across multiple reference
   * managers and allocators.
   *
   * @param decrement amount to decrease the reference count by
   * @return the new reference count
   */
  private int decrement(int decrement) {
    getAllocator().assertOpen();
    final int outcome;
    synchronized (getAllocationManager()) {
      outcome = bufRefCnt.addAndGet(-decrement);
      if (outcome == 0) {
        lDestructionTime = System.nanoTime();
        // refcount of this reference manager has dropped to 0
        // inform the allocation manager that this reference manager
        // no longer holds references to underlying memory
        getAllocationManager().release(this);
      }
    }
    return outcome;
  }

  /**
   * Decrement the ledger's reference count by 1 for the associated underlying
   * memory chunk. If the reference count drops to 0, it implies that
   * no ArrowBufs managed by this reference manager need access to the memory
   * chunk. In that case, the ledger should inform the allocation manager
   * about releasing its ownership for the chunk. Whether or not the memory
   * chunk will be released is something that {@link AllocationManager} will
   * decide since tracks the usage of memory chunk across multiple reference
   * managers and allocators.
   * @return true if the new ref count has dropped to 0, false otherwise
   */
  @Override
  public boolean release() {
    return release(1);
  }

  /**
   * Decrement the ledger's reference count for the associated underlying
   * memory chunk. If the reference count drops to 0, it implies that
   * no ArrowBufs managed by this reference manager need access to the memory
   * chunk. In that case, the ledger should inform the allocation manager
   * about releasing its ownership for the chunk. Whether or not the memory
   * chunk will be released is something that {@link AllocationManager} will
   * decide since tracks the usage of memory chunk across multiple reference
   * managers and allocators.
   * @param decrement amount to decrease the reference count by
   * @return true if the new ref count has dropped to 0, false otherwise
   */
  @Override
  public boolean release(int decrement) {
    Preconditions.checkState(decrement >= 1,
        "ref count decrement should be greater than or equal to 1");
    // decrement the ref count
    final int refCnt = decrement(decrement);
    if (BaseAllocator.DEBUG) {
      logEvent("release(%d). original value: %d",
          decrement, refCnt + decrement);
    }
    // the new ref count should be >= 0
    Preconditions.checkState(refCnt >= 0, "RefCnt has gone negative");
    return refCnt == 0;
  }

  /**
   * Increment the ledger's reference count for associated
   * underlying memory chunk by 1.
   */
  @Override
  public void retain() {
    retain(1);
  }

  /**
   * Increment the ledger's reference count for associated
   * underlying memory chunk by the given amount.
   *
   * @param increment amount to increase the reference count by
   */
  @Override
  public void retain(int increment) {
    Preconditions.checkArgument(increment > 0, "retain(%s) argument is not positive", increment);
    if (BaseAllocator.DEBUG) {
      logEvent("retain(%d)", increment);
    }
    final int originalReferenceCount = bufRefCnt.getAndAdd(increment);
    Preconditions.checkArgument(originalReferenceCount > 0);
  }



}
