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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.util.Preconditions;

/**
 * The abstract base class of MemoryChunkManager.
 *
 * <p>Manages the relationship between one or more allocators and a particular UDLE. Ensures that
 * one allocator owns the
 * memory that multiple allocators may be referencing. Manages a BufferLedger between each of its
 * associated allocators.
 *
 * <p>Threading: MemoryChunkManager manages thread-safety internally. Operations within the context
 * of a single BufferLedger
 * are lockless in nature and can be leveraged by multiple threads. Operations that cross the
 * context of two ledgers
 * will acquire a lock on the MemoryChunkManager instance. Important note, there is one
 * MemoryChunkManager per
 * MemoryChunk. As such, there will be thousands of these in a
 * typical query. The
 * contention of acquiring a lock on MemoryChunkManager should be very low.
 */
public class MemoryChunkManager {

  public static final Factory FACTORY = new Factory() {
    @Override
    public MemoryChunkManager create(MemoryChunk chunk) {
      return new MemoryChunkManager(chunk);
    }

    @Override
    public void close() {
      // no-op since this is the global factory
    }
  };

  // ARROW-1627 Trying to minimize memory overhead caused by previously used IdentityHashMap
  // see JIRA for details
  private final LowCostIdentityHashMap<BufferAllocator, BufferLedger> map = new LowCostIdentityHashMap<>();
  private final OwnerManager ownerManager = new OwnerManager();
  private final MemoryChunk chunk;
  private final AtomicBoolean trunkDestroyed = new AtomicBoolean(false);

  MemoryChunkManager(MemoryChunk chunk) {
    this.chunk = chunk;
  }

  OwnerManager getOwnerManager() {
    return ownerManager;
  }

  /**
   * Return the owner {@link BufferLedger}.
   *
   * @return the owner {@link BufferLedger}.
   */
  BufferLedger getOwningLedger() {
    return ownerManager.getOwner();
  }

  /**
   * Set owner {@link BufferLedger} for this {@link MemoryChunkManager}.
   *
   * @param ledger the owner {@link BufferLedger}.
   */
  void setOwningLedger(final BufferLedger ledger) {
    ownerManager.setOwner(ledger);
  }

  /**
   * Associate the existing underlying buffer with a new allocator. This will increase the
   * reference count on the corresponding buffer ledger by 1
   *
   * @param allocator The target allocator to associate this buffer with.
   * @return The reference manager (new or existing) that associates the underlying
   *         buffer to this new ledger.
   */
  BufferLedger associate(final BufferAllocator allocator) {
    allocator.assertOpen();
    synchronized (this) {
      if (!ownerManager.isSet()) {
        return ownerManager.setOwner(doAssociate(allocator));
      }
      Preconditions.checkState(ownerManager.getOwnerAllocator().getRoot() == allocator.getRoot(),
          "A buffer can only be associated between two allocators that share the same root");
      BufferLedger ledger = map.get(allocator);
      if (ledger != null) {
        // bump the ref count for the ledger
        ledger.increment();
        return ledger;
      }
      return doAssociate(allocator);
    }
  }

  void associateLedger(BufferAllocator allocator, BufferLedger ledger) {
    if (allocator instanceof BaseAllocator) {
      // needed for debugging only: keep a pointer to reference manager inside allocator
      // to dump state, verify allocator state etc
      ((BaseAllocator) allocator).associateLedger(ledger);
    }
  }

  private BufferLedger doAssociate(BufferAllocator allocator) {
    BufferLedger ledger = new BufferLedger(allocator, this);

    // the new reference manager will have a ref count of 1
    ledger.increment();

    // store the mapping for <allocator, reference manager>
    BufferLedger oldLedger = map.put(ledger);
    Preconditions.checkState(oldLedger == null,
        "Detected inconsistent state: A reference manager already exists for this allocator");

    associateLedger(allocator, ledger);
    return ledger;
  }

  void dissociateLedger(BufferAllocator allocator, BufferLedger ledger) {
    if (allocator instanceof BaseAllocator) {
      // needed for debug only: tell the allocator that MemoryChunkManager is removing a
      // reference manager associated with this particular allocator
      ((BaseAllocator) allocator).dissociateLedger(ledger);
    }
  }

  /**
   * The way that a particular ReferenceManager (BufferLedger) communicates back to the
   * MemoryChunkManager that it no longer needs to hold a reference to a particular
   * piece of memory. Reference manager needs to hold a lock to invoke this method
   * It is called when the shared refcount of all the ArrowBufs managed by the
   * calling ReferenceManager drops to 0.
   */
  void release(final BufferLedger ledger) {
    final BufferAllocator allocator = ledger.getAllocator();
    allocator.assertOpen();
    // remove the <BaseAllocator, BufferLedger> mapping for the allocator
    // of calling BufferLedger
    Preconditions.checkState(map.containsKey(allocator),
        "Expecting a mapping for allocator and reference manager");
    final BufferLedger oldLedger = map.remove(allocator);

    BufferAllocator oldAllocator = oldLedger.getAllocator();
    dissociateLedger(oldAllocator, oldLedger);

    if (ownerManager.isOwner(oldLedger)) {
      // the release call was made by the owning reference manager
      if (map.isEmpty()) {
        // the only <allocator, reference manager> mapping was for the owner
        // which now has been removed, it implies we can safely destroy the
        // underlying memory chunk as it is no longer being referenced
        doRelease();
      } else {
        // since the refcount dropped to 0 for the owning reference manager and allocation
        // manager will no longer keep a mapping for it, we need to change the owning
        // reference manager to whatever the next available <allocator, reference manager>
        // mapping exists.
        BufferLedger newOwningLedger = map.getNextValue();
        // we'll forcefully transfer the ownership and not worry about whether we
        // exceeded the limit since this consumer can't do anything with this.
        oldLedger.transferBalance(newOwningLedger);
      }
    } else {
      // the release call was made by a non-oledgerwning reference manager, so after remove there have
      // to be 1 or more <allocator, reference manager> mappings
      Preconditions.checkState(map.size() > 0,
          "The final removal of reference manager should be connected to owning reference manager");
    }
  }

  void doRelease() {
    final BufferAllocator owningAllocator = ownerManager.getOwnerAllocator();
    owningAllocator.releaseBytes(getSize());
    // free the memory chunk associated with the MemoryChunkManager
    destroyChunk();
    owningAllocator.getListener().onRelease(getSize());
    ownerManager.unset();
  }

  /**
   * Get the underlying {@link MemoryChunk}.
   *
   * @return the underlying memory chunk.
   */
  MemoryChunk getChunk() {
    return chunk;
  }

  /**
   * Release the underlying memory chunk.
   */
  void destroyChunk() {
    trunkDestroyed.set(true);
    chunk.destroy();
  }

  boolean isChunkDestroyed() {
    return trunkDestroyed.get();
  }

  /**
   * Return the size of underlying chunk of memory managed by this MemoryChunkManager.
   *
   * <p>The underlying memory chunk managed can be different from the original requested size.
   *
   * @return size of underlying memory chunk
   */
  long getSize() {
    return chunk.size();
  }

  /**
   * Return the absolute memory address pointing to the fist byte of underlying memory chunk.
   */
  long memoryAddress() {
    return chunk.memoryAddress();
  }

  /**
   * Manage owners of the underlying chunk.
   */
  static class OwnerManager {
    private volatile BufferLedger owner;
    private volatile List<Listener> listeners = null;

    private OwnerManager() {
      setOwner(null);
    }

    boolean isSet() {
      return owner != null;
    }

    boolean isOwner(BufferLedger leger) {
      if (!isSet()) {
        return false;
      }
      return leger == owner;
    }

    BufferLedger getOwner() {
      return owner;
    }

    BufferAllocator getOwnerAllocator() {
      return owner.getAllocator();
    }

    BufferLedger setOwner(BufferLedger owner) {
      this.owner = owner;
      if (listeners != null) {
        for (Listener listener : listeners) {
          listener.onOwnerChange(owner);
        }
      }
      return this.owner;
    }

    void unset() {
      setOwner(null);
    }

    void addListener(Listener listener) {
      if (listeners == null) {
        listeners = new ArrayList<>();
      }
      listeners.add(listener);
    }

    /**
     * Listener to listen on owner changing events.
     */
    interface Listener {
      void onOwnerChange(BufferLedger leger);
    }
  }

  /**
   * A factory interface for creating {@link MemoryChunkManager}.
   */
  public interface Factory {

    /**
     * Create an {@link MemoryChunkManager}.
     *
     * @param chunk The underlying {@link MemoryChunk} managed by the created {@link MemoryChunkManager}.
     * @return The created MemoryChunkManager.
     */
    MemoryChunkManager create(MemoryChunk chunk);

    /**
     * Close the Factory. This is called when the {@link BufferAllocator} is closed.
     */
    void close();
  }
}
