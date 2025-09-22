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
package io.delta.kernel.spark.utils;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;

/** A utility class for converting between Delta Kernel and Spark schemas and data types. */
public class SchemaUtils {

  //////////////////////
  // Kernel --> Spark //
  //////////////////////

  /** Converts a Delta Kernel schema to a Spark schema. */
  public static org.apache.spark.sql.types.StructType convertKernelSchemaToSparkSchema(
      StructType kernelSchema) {
    requireNonNull(kernelSchema);
    List<org.apache.spark.sql.types.StructField> fields = new ArrayList<>();

    for (StructField field : kernelSchema.fields()) {
      fields.add(
          new org.apache.spark.sql.types.StructField(
              field.getName(),
              convertKernelDataTypeToSparkDataType(field.getDataType()),
              field.isNullable(),
              // TODO: understand and plumb field metadata.
              Metadata.empty()));
    }

    return new org.apache.spark.sql.types.StructType(
        fields.toArray(new org.apache.spark.sql.types.StructField[0]));
  }

  /** Converts a Delta Kernel data type to a Spark data type. */
  public static org.apache.spark.sql.types.DataType convertKernelDataTypeToSparkDataType(
      DataType kernelDataType) {
    requireNonNull(kernelDataType);
    if (kernelDataType instanceof StringType) {
      return DataTypes.StringType;
    } else if (kernelDataType instanceof BooleanType) {
      return DataTypes.BooleanType;
    } else if (kernelDataType instanceof IntegerType) {
      return DataTypes.IntegerType;
    } else if (kernelDataType instanceof LongType) {
      return DataTypes.LongType;
    } else if (kernelDataType instanceof BinaryType) {
      return DataTypes.BinaryType;
    } else if (kernelDataType instanceof ByteType) {
      return DataTypes.ByteType;
    } else if (kernelDataType instanceof DateType) {
      return DataTypes.DateType;
    } else if (kernelDataType instanceof DecimalType) {
      DecimalType kernelDecimal = (DecimalType) kernelDataType;
      return DataTypes.createDecimalType(kernelDecimal.getPrecision(), kernelDecimal.getScale());
    } else if (kernelDataType instanceof DoubleType) {
      return DataTypes.DoubleType;
    } else if (kernelDataType instanceof FloatType) {
      return DataTypes.FloatType;
    } else if (kernelDataType instanceof ShortType) {
      return DataTypes.ShortType;
    } else if (kernelDataType instanceof TimestampType) {
      return DataTypes.TimestampType;
    } else if (kernelDataType instanceof TimestampNTZType) {
      return DataTypes.TimestampNTZType;
    } else if (kernelDataType instanceof ArrayType) {
      ArrayType kernelArray = (ArrayType) kernelDataType;
      return DataTypes.createArrayType(
          convertKernelDataTypeToSparkDataType(kernelArray.getElementType()),
          kernelArray.containsNull());
    } else if (kernelDataType instanceof MapType) {
      MapType kernelMap = (MapType) kernelDataType;
      return DataTypes.createMapType(
          convertKernelDataTypeToSparkDataType(kernelMap.getKeyType()),
          convertKernelDataTypeToSparkDataType(kernelMap.getValueType()),
          kernelMap.isValueContainsNull());
    } else if (kernelDataType instanceof StructType) {
      return convertKernelSchemaToSparkSchema((StructType) kernelDataType);
    } else {
      // TODO: add variant type, this requires upgrading spark version dependency to 4.0
      throw new IllegalArgumentException("unsupported data type " + kernelDataType);
    }
  }

  //////////////////////
  // Spark --> Kernel //
  //////////////////////

  /** Converts a Spark schema to a Delta Kernel schema. */
  public static StructType convertSparkSchemaToKernelSchema(
      org.apache.spark.sql.types.StructType sparkSchema) {
    requireNonNull(sparkSchema);
    List<StructField> kernelFields = new ArrayList<>();

    for (org.apache.spark.sql.types.StructField field : sparkSchema.fields()) {
      // TODO: understand and plumb field metadata.
      kernelFields.add(
          new StructField(
              field.name(),
              convertSparkDataTypeToKernelDataType(field.dataType()),
              field.nullable()));
    }

    return new StructType(kernelFields);
  }

  /** Converts a Spark data type to a Delta Kernel data type. */
  public static DataType convertSparkDataTypeToKernelDataType(
      org.apache.spark.sql.types.DataType sparkDataType) {
    requireNonNull(sparkDataType);
    if (sparkDataType instanceof org.apache.spark.sql.types.StringType) {
      return StringType.STRING;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.BooleanType) {
      return BooleanType.BOOLEAN;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.IntegerType) {
      return IntegerType.INTEGER;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.LongType) {
      return LongType.LONG;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.BinaryType) {
      return BinaryType.BINARY;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.ByteType) {
      return ByteType.BYTE;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.DateType) {
      return DateType.DATE;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.DecimalType) {
      org.apache.spark.sql.types.DecimalType sparkDecimal =
          (org.apache.spark.sql.types.DecimalType) sparkDataType;
      return new DecimalType(sparkDecimal.precision(), sparkDecimal.scale());
    } else if (sparkDataType instanceof org.apache.spark.sql.types.DoubleType) {
      return DoubleType.DOUBLE;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.FloatType) {
      return FloatType.FLOAT;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.ShortType) {
      return ShortType.SHORT;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.TimestampType) {
      return TimestampType.TIMESTAMP;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.TimestampNTZType) {
      return TimestampNTZType.TIMESTAMP_NTZ;
    } else if (sparkDataType instanceof org.apache.spark.sql.types.ArrayType) {
      org.apache.spark.sql.types.ArrayType sparkArray =
          (org.apache.spark.sql.types.ArrayType) sparkDataType;
      return new ArrayType(
          convertSparkDataTypeToKernelDataType(sparkArray.elementType()),
          sparkArray.containsNull());
    } else if (sparkDataType instanceof org.apache.spark.sql.types.MapType) {
      org.apache.spark.sql.types.MapType sparkMap =
          (org.apache.spark.sql.types.MapType) sparkDataType;
      return new MapType(
          convertSparkDataTypeToKernelDataType(sparkMap.keyType()),
          convertSparkDataTypeToKernelDataType(sparkMap.valueType()),
          sparkMap.valueContainsNull());
    } else if (sparkDataType instanceof org.apache.spark.sql.types.StructType) {
      return convertSparkSchemaToKernelSchema(
          (org.apache.spark.sql.types.StructType) sparkDataType);
    } else {
      throw new IllegalArgumentException("unsupported data type " + sparkDataType);
    }
  }

  //////////////////////
  // Validation Utils //
  //////////////////////

  /**
   * Validates that a Spark schema does not contain unsupported data types for kernel-spark.
   * Currently unsupported types:
   * - Variant
   * - String with non-default collation
   * - Time (upcoming in Spark 4.1)
   *
   * @param schema The Spark schema to validate
   * @param isFilePathAccess Whether this is file path access (true) or catalog access (false)
   * @throws UnsupportedDataTypeException if unsupported types are found and isFilePathAccess is true
   */
  public static void validateSchemaForKernelSpark(
      org.apache.spark.sql.types.StructType schema, boolean isFilePathAccess) {
    if (!isFilePathAccess) {
      // For catalog access, we don't validate since table access will be delegated to Spark
      return;
    }

    validateDataTypeForKernelSpark(schema, "root", isFilePathAccess);
  }

  /**
   * Recursively validates that a Spark data type does not contain unsupported types.
   */
  private static void validateDataTypeForKernelSpark(
      org.apache.spark.sql.types.DataType dataType, String fieldPath, boolean isFilePathAccess) {
    
    if (!isFilePathAccess) {
      return;
    }

    // Check for Variant type
    if (isVariantType(dataType)) {
      throw new UnsupportedDataTypeException(
          "Variant data type is not supported by the kernel-spark connector. " +
          "Found Variant type at field: " + fieldPath + ". " +
          "Please use a different data type or access the table through a catalog.");
    }

    // Check for String with non-default collation
    if (dataType instanceof org.apache.spark.sql.types.StringType) {
      org.apache.spark.sql.types.StringType stringType = 
          (org.apache.spark.sql.types.StringType) dataType;
      if (hasNonDefaultCollation(stringType)) {
        throw new UnsupportedDataTypeException(
            "String with non-default collation is not supported by the kernel-spark connector. " +
            "Found String with collation at field: " + fieldPath + ". " +
            "Please use default collation or access the table through a catalog.");
      }
    }

    // Check for Time type (when it becomes available in Spark 4.1)
    if (isTimeType(dataType)) {
      throw new UnsupportedDataTypeException(
          "Time data type is not supported by the kernel-spark connector. " +
          "Found Time type at field: " + fieldPath + ". " +
          "Please use a different data type or access the table through a catalog.");
    }

    // Recursively check complex types
    if (dataType instanceof org.apache.spark.sql.types.ArrayType) {
      org.apache.spark.sql.types.ArrayType arrayType = 
          (org.apache.spark.sql.types.ArrayType) dataType;
      validateDataTypeForKernelSpark(
          arrayType.elementType(), fieldPath + ".element", isFilePathAccess);
    } else if (dataType instanceof org.apache.spark.sql.types.MapType) {
      org.apache.spark.sql.types.MapType mapType = 
          (org.apache.spark.sql.types.MapType) dataType;
      validateDataTypeForKernelSpark(
          mapType.keyType(), fieldPath + ".key", isFilePathAccess);
      validateDataTypeForKernelSpark(
          mapType.valueType(), fieldPath + ".value", isFilePathAccess);
    } else if (dataType instanceof org.apache.spark.sql.types.StructType) {
      org.apache.spark.sql.types.StructType structType = 
          (org.apache.spark.sql.types.StructType) dataType;
      for (org.apache.spark.sql.types.StructField field : structType.fields()) {
        validateDataTypeForKernelSpark(
            field.dataType(), fieldPath + "." + field.name(), isFilePathAccess);
      }
    }
  }

  /**
   * Checks if a data type is a Variant type.
   * Uses reflection to be compatible with different Spark versions.
   */
  private static boolean isVariantType(org.apache.spark.sql.types.DataType dataType) {
    // Check if this is a Variant type by examining the class name
    // This approach works across different Spark versions
    String className = dataType.getClass().getSimpleName();
    return className.equals("VariantType") || className.contains("Variant");
  }

  /**
   * Checks if a StringType has non-default collation.
   * Uses reflection to be compatible with different Spark versions.
   */
  private static boolean hasNonDefaultCollation(org.apache.spark.sql.types.StringType stringType) {
    try {
      // In newer Spark versions, StringType may have collation information
      // For now, we assume default collation unless we can detect otherwise
      java.lang.reflect.Method getCollationMethod = 
          stringType.getClass().getMethod("collationId");
      Object collationId = getCollationMethod.invoke(stringType);
      // Default collation typically has ID 0
      return !collationId.equals(0);
    } catch (Exception e) {
      // If we can't determine collation (older Spark versions), assume default
      return false;
    }
  }

  /**
   * Checks if a data type is a Time type.
   * Uses reflection to be compatible with different Spark versions.
   */
  private static boolean isTimeType(org.apache.spark.sql.types.DataType dataType) {
    // Check if this is a Time type by examining the class name
    String className = dataType.getClass().getSimpleName();
    return className.equals("TimeType") || className.contains("Time");
  }

  /**
   * Exception thrown when unsupported data types are encountered.
   */
  public static class UnsupportedDataTypeException extends RuntimeException {
    public UnsupportedDataTypeException(String message) {
      super(message);
    }
  }
}
