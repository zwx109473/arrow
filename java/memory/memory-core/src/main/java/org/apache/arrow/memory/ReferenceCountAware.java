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
 * Base interface for reference counted facilities.
 */
public interface ReferenceCountAware {
  /**
   * Get current reference count.
   *
   * @return current reference count
   */
  int getRefCount();

  /**
   * Decrement reference count by 1.
   *
   * @return true if reference count has dropped to 0
   */
  boolean release();

  /**
   * Decrement reference count by specific amount of decrement.
   *
   * @param decrement the count to decrease the reference count by
   * @return true if reference count has dropped to 0
   */
  boolean release(int decrement);

  /**
   * Increment reference count by 1.
   */
  void retain();

  /**
   * Increment reference count by specific amount of increment.
   *
   * @param increment the count to increase the reference count by
   */
  void retain(int increment);
}
