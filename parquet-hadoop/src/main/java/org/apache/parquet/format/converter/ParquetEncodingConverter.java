/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.format.converter;

import org.apache.parquet.Log;
import org.apache.parquet.column.Encoding;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ParquetEncodingConverter {

  private static final Log LOG = Log.getLog(ParquetEncodingConverter.class);

  // NOTE: this cache is for memory savings, not cpu savings, and is used to de-duplicate
  // sets of encodings. It is important that all collections inserted to this cache be
  // immutable and have thread-safe read-only access. This can be achieved by wrapping
  // an unsynchronized collection in Collections.unmodifiable*(), and making sure to not
  // keep any references to the original collection.
  private static final ConcurrentHashMap<Set<Encoding>, Set<Encoding>>
      cachedEncodingSets = new ConcurrentHashMap<
      Set<org.apache.parquet.column.Encoding>, Set<org.apache.parquet.column.Encoding>>();

  public static org.apache.parquet.column.Encoding getEncoding(org.apache.parquet.format.Encoding
                                                             encoding) {
    return org.apache.parquet.column.Encoding.valueOf(encoding.name());
  }

  public static org.apache.parquet.format.Encoding getEncoding(org.apache.parquet.column.Encoding
                                                             encoding) {
    return org.apache.parquet.format.Encoding.valueOf(encoding.name());
  }

  static List<org.apache.parquet.format.Encoding> toFormatEncodings(Set<org.apache.parquet.column
      .Encoding> encodings) {
    List<org.apache.parquet.format.Encoding> converted = new ArrayList<org.apache.parquet.format.Encoding>(encodings.size());
    for (org.apache.parquet.column.Encoding encoding : encodings) {
      converted.add(ParquetEncodingConverter.getEncoding(encoding));
    }
    return converted;
  }

  // Visible for testing
  static Set<org.apache.parquet.column.Encoding> fromFormatEncodings(List<org.apache.parquet.format
      .Encoding> encodings) {
    Set<org.apache.parquet.column.Encoding> converted = new HashSet<Encoding>();

    for (org.apache.parquet.format.Encoding encoding : encodings) {
      converted.add(getEncoding(encoding));
    }

    // make converted unmodifiable, drop reference to modifiable copy
    converted = Collections.unmodifiableSet(converted);

    // atomically update the cache
    Set<org.apache.parquet.column.Encoding> cached = cachedEncodingSets.putIfAbsent(converted, converted);

    if (cached == null) {
      // cached == null signifies that converted was *not* in the cache previously
      // so we can return converted instead of throwing it away, it has now
      // been cached
      cached = converted;
    }

    return cached;
  }

  public static Set<Encoding> cacheIfAbsent(Set<Encoding> key, Set<Encoding> value) {
    return cachedEncodingSets.putIfAbsent(key, value);
  }
}
