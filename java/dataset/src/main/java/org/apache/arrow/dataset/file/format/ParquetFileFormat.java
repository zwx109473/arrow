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

import java.util.Collections;
import java.util.Map;

public class ParquetFileFormat extends FileFormatBase {
  private final String[] dictColumns;

  public ParquetFileFormat(String[] dictColumns) {
    super(createParquetFileFormat(dictColumns));
    this.dictColumns = dictColumns;
  }

  // Typically for Spark config parsing
  public static ParquetFileFormat create(Map<String, String> options) {
    return new ParquetFileFormat(parseStringArray(getOptionValue(options, "dictColumns", ""))
    );
  }

  public static ParquetFileFormat createDefault() {
    return create(Collections.emptyMap());
  }

  private static long createParquetFileFormat(String[] dictColumns) {
    return JniWrapper.get().createParquetFileFormat(dictColumns);
  }
}
