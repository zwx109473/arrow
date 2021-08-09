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

package org.apache.arrow.dataset.file.format;

import org.apache.arrow.dataset.file.JniWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

public abstract class FileFormatBase implements FileFormat {

  private final long nativeInstanceId;
  private boolean closed = false;

  public FileFormatBase(long nativeInstanceId) {
    this.nativeInstanceId = nativeInstanceId;
  }

  protected static String getOptionValue(Map<String, String> options, String key, String fallbackValue) {
    return options.getOrDefault(key, fallbackValue);
  }

  protected static String[] parseStringArray(String commaSeparated) {
    final StringTokenizer tokenizer = new StringTokenizer(commaSeparated, ",");
    final List<String> tokens = new ArrayList<>();
    while (tokenizer.hasMoreTokens()) {
      tokens.add(tokenizer.nextToken());
    }
    return tokens.toArray(new String[0]);
  }

  @Override
  public long id() {
    return nativeInstanceId;
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    JniWrapper.get().releaseFileFormatInstance(nativeInstanceId);
  }
}
