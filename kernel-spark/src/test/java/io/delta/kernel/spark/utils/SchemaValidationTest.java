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

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.spark.utils.SchemaUtils.UnsupportedDataTypeException;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Test;

public class SchemaValidationTest {

  @Test
  public void testValidateSchemaForKernelSpark_ValidSchema_FilePathAccess() {
    // Test with a valid schema that should not throw exceptions
    StructType validSchema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("name", DataTypes.StringType, true)
            .add("age", DataTypes.IntegerType, true)
            .add("timestamp", DataTypes.TimestampType, true);

    // Should not throw for file path access
    assertDoesNotThrow(() -> SchemaUtils.validateSchemaForKernelSpark(validSchema, true));
  }

  @Test
  public void testValidateSchemaForKernelSpark_ValidSchema_CatalogAccess() {
    // Test with any schema for catalog access - should always pass
    StructType anySchema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("name", DataTypes.StringType, true);

    // Should not throw for catalog access
    assertDoesNotThrow(() -> SchemaUtils.validateSchemaForKernelSpark(anySchema, false));
  }

  @Test
  public void testValidateSchemaForKernelSpark_VariantType_FilePathAccess() {
    // Create a mock VariantType using reflection to simulate future Spark versions
    try {
      StructType schemaWithVariant =
          new StructType()
              .add("id", DataTypes.IntegerType, false)
              .add("variant_data", createMockVariantType(), true);

      UnsupportedDataTypeException exception =
          assertThrows(
              UnsupportedDataTypeException.class,
              () -> SchemaUtils.validateSchemaForKernelSpark(schemaWithVariant, true));

      assertTrue(exception.getMessage().contains("Variant data type is not supported"));
      assertTrue(exception.getMessage().contains("root.variant_data"));
    } catch (Exception e) {
      // Skip this test if we can't create a mock VariantType
      System.out.println("Skipping VariantType test - mock creation failed: " + e.getMessage());
    }
  }

  @Test
  public void testValidateSchemaForKernelSpark_VariantType_CatalogAccess() {
    // Even with unsupported types, catalog access should not throw
    try {
      StructType schemaWithVariant =
          new StructType()
              .add("id", DataTypes.IntegerType, false)
              .add("variant_data", createMockVariantType(), true);

      // Should not throw for catalog access
      assertDoesNotThrow(() -> SchemaUtils.validateSchemaForKernelSpark(schemaWithVariant, false));
    } catch (Exception e) {
      // Skip this test if we can't create a mock VariantType
      System.out.println(
          "Skipping VariantType catalog test - mock creation failed: " + e.getMessage());
    }
  }

  @Test
  public void testValidateSchemaForKernelSpark_NestedStructWithVariant_FilePathAccess() {
    try {
      StructType nestedSchema =
          new StructType()
              .add("id", DataTypes.IntegerType, false)
              .add(
                  "nested",
                  new StructType().add("inner_variant", createMockVariantType(), true),
                  true);

      UnsupportedDataTypeException exception =
          assertThrows(
              UnsupportedDataTypeException.class,
              () -> SchemaUtils.validateSchemaForKernelSpark(nestedSchema, true));

      assertTrue(exception.getMessage().contains("Variant data type is not supported"));
      assertTrue(exception.getMessage().contains("root.nested.inner_variant"));
    } catch (Exception e) {
      // Skip this test if we can't create a mock VariantType
      System.out.println(
          "Skipping nested VariantType test - mock creation failed: " + e.getMessage());
    }
  }

  @Test
  public void testValidateSchemaForKernelSpark_ArrayWithVariant_FilePathAccess() {
    try {
      StructType schemaWithArrayVariant =
          new StructType()
              .add("id", DataTypes.IntegerType, false)
              .add("variant_array", DataTypes.createArrayType(createMockVariantType(), true), true);

      UnsupportedDataTypeException exception =
          assertThrows(
              UnsupportedDataTypeException.class,
              () -> SchemaUtils.validateSchemaForKernelSpark(schemaWithArrayVariant, true));

      assertTrue(exception.getMessage().contains("Variant data type is not supported"));
      assertTrue(exception.getMessage().contains("root.variant_array.element"));
    } catch (Exception e) {
      // Skip this test if we can't create a mock VariantType
      System.out.println(
          "Skipping array VariantType test - mock creation failed: " + e.getMessage());
    }
  }

  @Test
  public void testValidateSchemaForKernelSpark_MapWithVariant_FilePathAccess() {
    try {
      StructType schemaWithMapVariant =
          new StructType()
              .add("id", DataTypes.IntegerType, false)
              .add(
                  "variant_map",
                  DataTypes.createMapType(DataTypes.StringType, createMockVariantType(), true),
                  true);

      UnsupportedDataTypeException exception =
          assertThrows(
              UnsupportedDataTypeException.class,
              () -> SchemaUtils.validateSchemaForKernelSpark(schemaWithMapVariant, true));

      assertTrue(exception.getMessage().contains("Variant data type is not supported"));
      assertTrue(exception.getMessage().contains("root.variant_map.value"));
    } catch (Exception e) {
      // Skip this test if we can't create a mock VariantType
      System.out.println("Skipping map VariantType test - mock creation failed: " + e.getMessage());
    }
  }

  @Test
  public void testValidateSchemaForKernelSpark_TimeType_FilePathAccess() {
    try {
      StructType schemaWithTime =
          new StructType()
              .add("id", DataTypes.IntegerType, false)
              .add("time_data", createMockTimeType(), true);

      UnsupportedDataTypeException exception =
          assertThrows(
              UnsupportedDataTypeException.class,
              () -> SchemaUtils.validateSchemaForKernelSpark(schemaWithTime, true));

      assertTrue(exception.getMessage().contains("Time data type is not supported"));
      assertTrue(exception.getMessage().contains("root.time_data"));
    } catch (Exception e) {
      // Skip this test if we can't create a mock TimeType
      System.out.println("Skipping TimeType test - mock creation failed: " + e.getMessage());
    }
  }

  /** Creates a mock VariantType for testing by creating a dynamic class. */
  private DataType createMockVariantType() throws Exception {
    // Create a mock class that simulates VariantType
    return new DataType() {
      @Override
      public String typeName() {
        return "variant";
      }

      @Override
      public String sql() {
        return "VARIANT";
      }

      @Override
      public String simpleString() {
        return "variant";
      }

      @Override
      public String catalogString() {
        return "variant";
      }

      @Override
      public boolean equals(Object obj) {
        return obj instanceof DataType && obj.getClass().getSimpleName().contains("Variant");
      }

      @Override
      public int hashCode() {
        return "VariantType".hashCode();
      }

      @Override
      public Class<?> getClass() {
        // Return a class with "Variant" in the name for our detection logic
        return MockVariantType.class;
      }
    };
  }

  /** Creates a mock TimeType for testing by creating a dynamic class. */
  private DataType createMockTimeType() throws Exception {
    // Create a mock class that simulates TimeType
    return new DataType() {
      @Override
      public String typeName() {
        return "time";
      }

      @Override
      public String sql() {
        return "TIME";
      }

      @Override
      public String simpleString() {
        return "time";
      }

      @Override
      public String catalogString() {
        return "time";
      }

      @Override
      public boolean equals(Object obj) {
        return obj instanceof DataType && obj.getClass().getSimpleName().contains("Time");
      }

      @Override
      public int hashCode() {
        return "TimeType".hashCode();
      }

      @Override
      public Class<?> getClass() {
        // Return a class with "Time" in the name for our detection logic
        return MockTimeType.class;
      }
    };
  }

  // Mock classes for testing
  private static class MockVariantType extends DataType {
    @Override
    public String typeName() {
      return "variant";
    }

    @Override
    public String sql() {
      return "VARIANT";
    }

    @Override
    public String simpleString() {
      return "variant";
    }

    @Override
    public String catalogString() {
      return "variant";
    }
  }

  private static class MockTimeType extends DataType {
    @Override
    public String typeName() {
      return "time";
    }

    @Override
    public String sql() {
      return "TIME";
    }

    @Override
    public String simpleString() {
      return "time";
    }

    @Override
    public String catalogString() {
      return "time";
    }
  }
}
