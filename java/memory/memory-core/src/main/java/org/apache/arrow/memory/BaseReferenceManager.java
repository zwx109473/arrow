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
 * Standard implementation of {@link ReferenceManager} backed by a
 * {@link BufferLedger}.
 */
public class BaseReferenceManager implements ReferenceManager {
  private final BufferLedger ledger;
  private final BufferAllocator allocator;
  private final AllocationManager allocationManager;

  public BaseReferenceManager(BufferLedger ledger) {
    this.ledger = ledger;
    this.allocator = ledger.getAllocator();
    this.allocationManager = ledger.getAllocationManager();
  }

  @Override
  public int getRefCount() {
    return ledger.getRefCount();
  }

  @Override
  public boolean release() {
    return ledger.release();
  }

  @Override
  public boolean release(int decrement) {
    return ledger.release(decrement);
  }

  @Override
  public void retain() {
    ledger.retain();
  }

  @Override
  public void retain(int increment) {
    ledger.retain(increment);
  }

  /**
   * Derive a new ArrowBuf from a given source ArrowBuf. The new derived
   * ArrowBuf will share the same reference count as rest of the ArrowBufs
   * associated with this ledger. This operation is typically used for
   * slicing -- creating new ArrowBufs from a compound ArrowBuf starting at
   * a particular index in the underlying memory and having access to a
   * particular length (in bytes) of data in memory chunk.
   * <p>
   * This method is also used as a helper for transferring ownership and retain to target
   * allocator.
   * </p>
   * @param sourceBuffer source ArrowBuf
   * @param index index (relative to source ArrowBuf) new ArrowBuf should be
   *              derived from
   * @param length length (bytes) of data in underlying memory that derived buffer will
   *               have access to in underlying memory
   * @return derived buffer
   */
  @Override
  public ArrowBuf deriveBuffer(final ArrowBuf sourceBuffer, long index, long length) {
    return ledger.deriveBuffer(sourceBuffer, index, length);
  }

  /**
   * Create a new ArrowBuf that is associated with an alternative allocator for the purposes of
   * memory ownership and accounting. This has no impact on the reference counting for the current
   * ArrowBuf except in the situation where the passed in Allocator is the same as the current buffer.
   * <p>
   * This operation has no impact on the reference count of this ArrowBuf. The newly created
   * ArrowBuf with either have a reference count of 1 (in the case that this is the first time this
   * memory is being associated with the target allocator or in other words allocation manager currently
   * doesn't hold a mapping for the target allocator) or the current value of the reference count for
   * the target allocator-reference manager combination + 1 in the case that the provided allocator
   * already had an association to this underlying memory.
   * </p>
   *
   * @param srcBuffer source ArrowBuf
   * @param target The target allocator to create an association with.
   * @return A new ArrowBuf which shares the same underlying memory as the provided ArrowBuf.
   */
  @Override
  public ArrowBuf retain(final ArrowBuf srcBuffer, BufferAllocator target) {

    if (BaseAllocator.DEBUG) {
      ledger.logEvent("retain(%s)", target.getName());
    }

    // the call to associate will return the corresponding reference manager (buffer ledger) for
    // the target allocator. if the allocation manager didn't already have a mapping
    // for the target allocator, it will create one and return the new reference manager with a
    // reference count of 1. Thus the newly created buffer in this case will have a ref count of 1.
    // alternatively, if there was already a mapping for <buffer allocator, ref manager> in
    // allocation manager, the ref count of the new buffer will be targetrefmanager.refcount() + 1
    // and this will be true for all the existing buffers currently managed by targetrefmanager
    final BufferLedger targetRefManager = allocationManager.associate(target);
    // create a new ArrowBuf to associate with new allocator and target ref manager
    final long targetBufLength = srcBuffer.capacity();
    ArrowBuf targetArrowBuf = targetRefManager.deriveBuffer(srcBuffer, 0, targetBufLength);
    targetArrowBuf.readerIndex(srcBuffer.readerIndex());
    targetArrowBuf.writerIndex(srcBuffer.writerIndex());
    return targetArrowBuf;
  }

  /**
   * Transfer the memory accounting ownership of this ArrowBuf to another allocator.
   * This will generate a new ArrowBuf that carries an association with the underlying memory
   * of this ArrowBuf. If this ArrowBuf is connected to the owning BufferLedger of this memory,
   * that memory ownership/accounting will be transferred to the target allocator. If this
   * ArrowBuf does not currently own the memory underlying it (and is only associated with it),
   * this does not transfer any ownership to the newly created ArrowBuf.
   * <p>
   * This operation has no impact on the reference count of this ArrowBuf. The newly created
   * ArrowBuf with either have a reference count of 1 (in the case that this is the first time
   * this memory is being associated with the new allocator) or the current value of the reference
   * count for the other AllocationManager/BufferLedger combination + 1 in the case that the provided
   * allocator already had an association to this underlying memory.
   * </p>
   * <p>
   * Transfers will always succeed, even if that puts the other allocator into an overlimit
   * situation. This is possible due to the fact that the original owning allocator may have
   * allocated this memory out of a local reservation whereas the target allocator may need to
   * allocate new memory from a parent or RootAllocator. This operation is done n a mostly-lockless
   * but consistent manner. As such, the overlimit==true situation could occur slightly prematurely
   * to an actual overlimit==true condition. This is simply conservative behavior which means we may
   * return overlimit slightly sooner than is necessary.
   * </p>
   *
   * @param target The allocator to transfer ownership to.
   * @return A new transfer result with the impact of the transfer (whether it was overlimit) as
   *         well as the newly created ArrowBuf.
   */
  @Override
  public TransferResult transferOwnership(final ArrowBuf srcBuffer, final BufferAllocator target) {
    // the call to associate will return the corresponding reference manager (buffer ledger) for
    // the target allocator. if the allocation manager didn't already have a mapping
    // for the target allocator, it will create one and return the new reference manager with a
    // reference count of 1. Thus the newly created buffer in this case will have a ref count of 1.
    // alternatively, if there was already a mapping for <buffer allocator, ref manager> in
    // allocation manager, the ref count of the new buffer will be targetrefmanager.refcount() + 1
    // and this will be true for all the existing buffers currently managed by targetrefmanager
    final BufferLedger targetLedger = allocationManager.associate(target);
    // create a new ArrowBuf to associate with new allocator and target ref manager
    final long targetBufLength = srcBuffer.capacity();
    final ArrowBuf targetArrowBuf = targetLedger.deriveBuffer(srcBuffer, 0, targetBufLength);
    targetArrowBuf.readerIndex(srcBuffer.readerIndex());
    targetArrowBuf.writerIndex(srcBuffer.writerIndex());
    final boolean allocationFit = ledger.transferBalance(targetLedger);
    return new TransferResult(allocationFit, targetArrowBuf);
  }

  @Override
  public BufferAllocator getAllocator() {
    return ledger.getAllocator();
  }

  @Override
  public long getSize() {
    return ledger.getSize();
  }

  @Override
  public long getAccountedSize() {
    return ledger.getAccountedSize();
  }

  /**
   * The outcome of a Transfer.
   */
  public static class TransferResult implements OwnershipTransferResult {

    // Whether this transfer fit within the target allocator's capacity.
    final boolean allocationFit;

    // The newly created buffer associated with the target allocator
    public final ArrowBuf buffer;

    private TransferResult(boolean allocationFit, ArrowBuf buffer) {
      this.allocationFit = allocationFit;
      this.buffer = buffer;
    }

    @Override
    public ArrowBuf getTransferredBuffer() {
      return buffer;
    }

    @Override
    public boolean getAllocationFit() {
      return allocationFit;
    }
  }
}
