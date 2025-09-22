/*
 * Copyright (2025) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.spark.read;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.spark.utils.SchemaUtils;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A Spark ScanBuilder implementation that wraps Delta Kernel's ScanBuilder. This allows Spark to
 * use Delta Kernel for reading Delta tables.
 */
public class SparkScanBuilder implements ScanBuilder, SupportsPushDownRequiredColumns {

  private final io.delta.kernel.ScanBuilder kernelScanBuilder;
  private final String tablePath;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final CaseInsensitiveStringMap options;
  private final Set<String> partitionColumnSet;
  private StructType requiredDataSchema;
  private Predicate[] pushedPredicates;

  public SparkScanBuilder(
      String tableName,
      String tablePath,
      StructType dataSchema,
      StructType partitionSchema,
      SnapshotImpl snapshot,
      CaseInsensitiveStringMap options) {
    this.kernelScanBuilder = requireNonNull(snapshot, "snapshot is null").getScanBuilder();
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.dataSchema = requireNonNull(dataSchema, "dataSchema is null");
    this.partitionSchema = requireNonNull(partitionSchema, "partitionSchema is null");
    this.options = requireNonNull(options, "options is null");
    this.requiredDataSchema = this.dataSchema;
    this.partitionColumnSet =
        Arrays.stream(this.partitionSchema.fields())
            .map(f -> f.name().toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet());
    this.pushedPredicates = new Predicate[0];

    // Validate schema for unsupported data types
    // Determine if this is file path access vs catalog access
    boolean isFilePathAccess = isFilePathAccess(tableName, tablePath);
    SchemaUtils.validateSchemaForKernelSpark(dataSchema, isFilePathAccess);
    SchemaUtils.validateSchemaForKernelSpark(partitionSchema, isFilePathAccess);
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    requireNonNull(requiredSchema, "requiredSchema is null");
    this.requiredDataSchema =
        new StructType(
            Arrays.stream(requiredSchema.fields())
                .filter(f -> !partitionColumnSet.contains(f.name().toLowerCase(Locale.ROOT)))
                .toArray(StructField[]::new));
  }

  @Override
  public org.apache.spark.sql.connector.read.Scan build() {
    // TODO: Implement predicate pushdown by translating Spark Filters to Delta Kernel Predicates.
    return new SparkScan(
        tablePath,
        dataSchema,
        partitionSchema,
        requiredDataSchema,
        pushedPredicates,
        new Filter[0],
        kernelScanBuilder.build(),
        options);
  }

  CaseInsensitiveStringMap getOptions() {
    return options;
  }

  StructType getDataSchema() {
    return dataSchema;
  }

  StructType getPartitionSchema() {
    return partitionSchema;
  }

  /**
   * Determines whether this is file path access (true) or catalog access (false).
   *
   * <p>We consider it file path access if: - tableName is null, empty, or looks like a file path -
   * tableName equals tablePath (indicating direct file path usage)
   *
   * <p>We consider it catalog access if: - tableName is a valid identifier that doesn't look like a
   * path
   */
  private boolean isFilePathAccess(String tableName, String tablePath) {
    if (tableName == null || tableName.trim().isEmpty()) {
      return true;
    }

    // If tableName looks like a file path (contains path separators), it's file path access
    if (tableName.contains("/") || tableName.contains("\\")) {
      return true;
    }

    // If tableName equals tablePath, it's likely file path access
    if (tableName.equals(tablePath)) {
      return true;
    }

    // If tableName starts with common file system schemes, it's file path access
    String lowerTableName = tableName.toLowerCase();
    if (lowerTableName.startsWith("file:")
        || lowerTableName.startsWith("hdfs:")
        || lowerTableName.startsWith("s3:")
        || lowerTableName.startsWith("s3a:")
        || lowerTableName.startsWith("s3n:")
        || lowerTableName.startsWith("gs:")
        || lowerTableName.startsWith("abfs:")
        || lowerTableName.startsWith("adls:")) {
      return true;
    }

    // Otherwise, assume it's catalog access (tableName is a proper table identifier)
    return false;
  }
}
