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

import java.util.IdentityHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.memory.util.CommonUtil;
import org.apache.arrow.memory.util.HistoricalLog;
import org.apache.arrow.util.Preconditions;

/**
 * The reference manager that binds an {@link AllocationManager} to
 * {@link BufferAllocator} and a set of {@link ArrowBuf}. The set of
 * ArrowBufs managed by this reference manager share a common
 * fate (same reference count).
 */
public abstract class BufferLedger implements ValueWithKeyIncluded<BufferAllocator>, ReferenceCountAware {
  private final IdentityHashMap<ArrowBuf.Logger, Object> buffers =
          BaseAllocator.DEBUG ? new IdentityHashMap<>() : null;
  private static final AtomicLong LEDGER_ID_GENERATOR = new AtomicLong(0);
  // unique ID assigned to each ledger
  private final long ledgerId = LEDGER_ID_GENERATOR.incrementAndGet();
  // manage request for retain
  // correctly
  private final long lCreationTime = System.nanoTime();
  private final BufferAllocator allocator;
  private final AllocationManager allocationManager;
  private final HistoricalLog historicalLog =
      BaseAllocator.DEBUG ? new HistoricalLog(BaseAllocator.DEBUG_LOG_LENGTH,
        "BufferLedger[%d]", 1) : null;

  BufferLedger(final BufferAllocator allocator, final AllocationManager allocationManager) {
    this.allocator = allocator;
    this.allocationManager = allocationManager;
  }

  /**
   * Increment the ledger's reference count for the associated
   * underlying memory chunk. All ArrowBufs managed by this ledger
   * will share the ref count.
   */
  protected abstract void increment();


  /**
   * Get destruction time of this buffer ledger.
   *
   * @return destruction time in nano, 0 if the ledger is not destructed yet
   */
  protected abstract long getDestructionTime();


  /**
   * Create new instance of {@link ReferenceManager} using this ledger.
   *
   * @return the newly created instance of {@link ReferenceManager}
   */
  protected abstract ReferenceManager newReferenceManager();

  /**
   * Used by an allocator to create a new ArrowBuf. This is provided
   * as a helper method for the allocator when it allocates a new memory chunk
   * using a new instance of allocation manager and creates a new reference manager
   * too.
   *
   * @param length  The length in bytes that this ArrowBuf will provide access to.
   * @param manager An optional BufferManager argument that can be used to manage expansion of
   *                this ArrowBuf
   * @return A new ArrowBuf that shares references with all ArrowBufs associated
   *         with this BufferLedger
   */
  ArrowBuf newArrowBuf(final long length, final BufferManager manager) {
    allocator.assertOpen();

    // the start virtual address of the ArrowBuf will be same as address of memory chunk
    final long startAddress = allocationManager.memoryAddress();

    // create ArrowBuf
    final ArrowBuf buf = new ArrowBuf(newReferenceManager(), manager, length, startAddress);

    // logging
    if (BaseAllocator.DEBUG) {
      logEvent(
          "ArrowBuf(BufferLedger, BufferAllocator[%s], " +
          "UnsafeDirectLittleEndian[identityHashCode == " + "%d](%s)) => ledger hc == %d",
          allocator.getName(), System.identityHashCode(buf), buf.toString(),
          System.identityHashCode(this));

      synchronized (buffers) {
        buffers.put(buf.createLogger(), null);
      }
    }

    return buf;
  }

  ArrowBuf deriveBuffer(final ArrowBuf sourceBuffer, long index, long length) {
    /*
     * Usage type 1 for deriveBuffer():
     * Used for slicing where index represents a relative index in the source ArrowBuf
     * as the slice start point. This is why we need to add the source buffer offset
     * to compute the start virtual address of derived buffer within the
     * underlying chunk.
     *
     * Usage type 2 for deriveBuffer():
     * Used for retain(target allocator) and transferOwnership(target allocator)
     * where index is 0 since these operations simply create a new ArrowBuf associated
     * with another combination of allocator buffer ledger for the same underlying memory
     */

    // the memory address stored inside ArrowBuf is its starting virtual
    // address in the underlying memory chunk from the point it has
    // access. so it is already accounting for the offset of the source buffer
    // we can simply add the index to get the starting address of new buffer.
    final long derivedBufferAddress = sourceBuffer.memoryAddress() + index;

    // create new ArrowBuf
    final ArrowBuf derivedBuf = new ArrowBuf(
            newReferenceManager(),
            null,
            length, // length (in bytes) in the underlying memory chunk for this new ArrowBuf
            derivedBufferAddress // starting byte address in the underlying memory for this new ArrowBuf
            );

    // logging
    if (BaseAllocator.DEBUG) {
      logEvent(
              "ArrowBuf(BufferLedger, BufferAllocator[%s], " +
                      "UnsafeDirectLittleEndian[identityHashCode == " +
                      "%d](%s)) => ledger hc == %d",
              allocator.getName(), System.identityHashCode(derivedBuf), derivedBuf.toString(),
              System.identityHashCode(this));

      synchronized (buffers) {
        buffers.put(derivedBuf.createLogger(), null);
      }
    }

    return derivedBuf;
  }

  /**
   * Transfer any balance the current ledger has to the target ledger. In the case
   * that the current ledger holds no memory, no transfer is made to the new ledger.
   *
   * @param targetLedger The ledger to transfer ownership account to.
   * @return Whether transfer fit within target ledgers limits.
   */
  boolean transferBalance(final BufferLedger targetLedger) {
    Preconditions.checkArgument(targetLedger != null,
        "Expecting valid target reference manager");
    final BufferAllocator targetAllocator = targetLedger.getAllocator();
    Preconditions.checkArgument(allocator.getRoot() == targetAllocator.getRoot(),
        "You can only transfer between two allocators that share the same root.");

    allocator.assertOpen();
    targetLedger.getAllocator().assertOpen();

    // if we're transferring to ourself, just return.
    if (targetLedger == this) {
      return true;
    }

    // since two balance transfers out from the allocation manager could cause incorrect
    // accounting, we need to ensure
    // that this won't happen by synchronizing on the allocation manager instance.
    synchronized (allocationManager) {
      if (allocationManager.getOwningLedger() != this) {
        // since the calling reference manager is not the owning
        // reference manager for the underlying memory, transfer is
        // a NO-OP
        return true;
      }

      if (BaseAllocator.DEBUG) {
        logEvent("transferBalance(%s)",
            targetLedger.getAllocator().getName());
      }

      boolean overlimit = targetAllocator.forceAllocate(allocationManager.getSize());
      allocator.releaseBytes(allocationManager.getSize());
      // since the transfer can only happen from the owning reference manager,
      // we need to set the target ref manager as the new owning ref manager
      // for the chunk of memory in allocation manager
      allocationManager.setOwningLedger(targetLedger);
      return overlimit;
    }
  }

  /**
   * Total size (in bytes) of memory underlying this reference manager.
   * @return Size (in bytes) of the memory chunk
   */
  public long getSize() {
    return allocationManager.getSize();
  }

  /**
   * How much memory is accounted for by this ledger. This is either getSize()
   * if this is the owning ledger for the memory or zero in the case that this
   * is not the owning ledger associated with this memory.
   * @return Amount of accounted(owned) memory associated with this ledger.
   */
  public long getAccountedSize() {
    synchronized (allocationManager) {
      if (allocationManager.getOwningLedger() == this) {
        return allocationManager.getSize();
      } else {
        return 0;
      }
    }
  }

  /**
   * Print the current ledger state to the provided StringBuilder.
   *
   * @param sb        The StringBuilder to populate.
   * @param indent    The level of indentation to position the data.
   * @param verbosity The level of verbosity to print.
   */
  void print(StringBuilder sb, int indent, BaseAllocator.Verbosity verbosity) {
    CommonUtil.indent(sb, indent)
      .append("ledger[")
      .append(ledgerId)
      .append("] allocator: ")
      .append(allocator.getName())
      .append("), isOwning: ")
      .append(", size: ")
      .append(", references: ")
      .append(getRefCount())
      .append(", life: ")
      .append(lCreationTime)
      .append("..")
      .append(getDestructionTime())
      .append(", allocatorManager: [")
      .append(", life: ");

    if (!BaseAllocator.DEBUG) {
      sb.append("]\n");
    } else {
      synchronized (buffers) {
        sb.append("] holds ")
          .append(buffers.size())
          .append(" buffers. \n");
        for (ArrowBuf.Logger bufLogger : buffers.keySet()) {
          bufLogger.print(sb, indent + 2, verbosity);
          sb.append('\n');
        }
      }
    }
  }

  /**
   * Get the {@link AllocationManager} used by this BufferLedger.
   *
   * @return The AllocationManager used by this BufferLedger.
   */
  AllocationManager getAllocationManager() {
    return allocationManager;
  }

  /**
   * Record a single log line into this ledger's historical log.
   */
  protected void logEvent(final String noteFormat, Object... args) {
    historicalLog.recordEvent(noteFormat, args);
  }

  /**
   * If this ledger is the owning ledger of the underlying allocation manager.
   *
   * @return true if this ledger owns its allocation manager
   */
  boolean isOwningLedger() {
    return this == allocationManager.getOwningLedger();
  }

  /**
   * Get the buffer allocator associated with this reference manager.
   * @return buffer allocator
   */
  BufferAllocator getAllocator() {
    return allocator;
  }

  /**
   * Get allocator key. Used by {@link LowCostIdentityHashMap}.
   */
  public BufferAllocator getKey() {
    return allocator;
  }

  /**
   * A factory interface for creating {@link BufferLedger}.
   */
  public interface Factory {
    /**
     * Create an instance of {@link BufferLedger}.
     *
     * @param allocator The allocator that will bind to the newly created {@link BufferLedger}.
     * @param allocationManager The {@link AllocationManager} that actually holds the underlying
     *                          memory block. Note that the newly created {@link BufferLedger} will
     *                          not be the one that actually owns this piece of memory by default.
     * @return The created {@link BufferLedger}.
     */
    BufferLedger create(BufferAllocator allocator, AllocationManager allocationManager);
  }
}
