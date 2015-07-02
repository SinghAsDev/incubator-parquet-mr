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

import org.apache.parquet.CorruptStatistics;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.schema.PrimitiveType;

public class ParquetStatisticsConverter {
  public static Statistics toParquetStatistics(
      org.apache.parquet.column.statistics.Statistics statistics) {
    Statistics stats = new Statistics();
    if (!statistics.isEmpty()) {
      stats.setNull_count(statistics.getNumNulls());
      if (statistics.hasNonNullValue()) {
        stats.setMax(statistics.getMaxBytes());
        stats.setMin(statistics.getMinBytes());
      }
    }
    return stats;
  }

  public static org.apache.parquet.column.statistics.Statistics fromParquetStatistics
      (String createdBy, Statistics statistics, PrimitiveType.PrimitiveTypeName type) {
    // create stats object based on the column type
    org.apache.parquet.column.statistics.Statistics stats = org.apache.parquet.column.statistics.Statistics.getStatsBasedOnType(type);
    // If there was no statistics written to the footer, create an empty Statistics object and return

    // NOTE: See docs in CorruptStatistics for explanation of why this check is needed
    if (statistics != null && !CorruptStatistics.shouldIgnoreStatistics(createdBy, type)) {
      if (statistics.isSetMax() && statistics.isSetMin()) {
        stats.setMinMaxFromBytes(statistics.min.array(), statistics.max.array());
      }
      stats.setNumNulls(statistics.null_count);
    }
    return stats;
  }
}
