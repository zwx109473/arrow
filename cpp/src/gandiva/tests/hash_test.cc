// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <sstream>

#include <arrow/pretty_print.h>
#include <gtest/gtest.h>

#include <sstream>

#include <cmath>
#include "arrow/memory_pool.h"
#include "arrow/status.h"

#include <iostream>
#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::float32;
using arrow::float64;
using arrow::int32;
using arrow::int64;
using arrow::utf8;

class TestHash : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestHash, TestSimple) {
  // schema for input fields
  auto field_a = field("a", int32());
  auto field_b = field("b", int32());
  auto field_c = field("c", int64());
  auto field_d = field("d", arrow::float32());
  auto field_e = field("e", arrow::float64());
  auto field_f = field("f", arrow::boolean());
  auto schema = arrow::schema({field_a, field_b, field_c, field_d, field_e, field_f});

  // output fields
  auto res_0 = field("res0", int32());
  auto res_1 = field("res1", int64());
  auto res_2 = field("res2", int32());
  auto res_3 = field("res3", int32());
  auto res_4 = field("res4", int32());
  auto res_5 = field("res5", int32());
  auto res_6 = field("res6", int32());

  // build expression.
  // hash32(a, 10)
  // hash64(a)
  float java_fNaN;
  double java_dNaN;
  int32_t java_fNaN_raw = 2143289344;
  uint64_t java_dNaN_raw = 0x7ff8000000000000;
  memcpy(&java_fNaN, &java_fNaN_raw, 4);
  memcpy(&java_dNaN, &java_dNaN_raw, 8);
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto node_c = TreeExprBuilder::MakeField(field_c);
  auto node_d = TreeExprBuilder::MakeField(field_d);
  auto node_e = TreeExprBuilder::MakeField(field_e);
  auto node_f = TreeExprBuilder::MakeField(field_f);
  auto literal_10 = TreeExprBuilder::MakeLiteral((int32_t)10);
  auto literal_0 = TreeExprBuilder::MakeLiteral((int32_t)0);
  auto literal_dn0 = TreeExprBuilder::MakeLiteral((double)-0.0);
  auto literal_d0 = TreeExprBuilder::MakeLiteral((double)0.0);
  auto literal_Java_fNaN = TreeExprBuilder::MakeLiteral(java_fNaN);
  auto literal_Java_dNaN = TreeExprBuilder::MakeLiteral(java_dNaN);
  auto isnan_f = TreeExprBuilder::MakeFunction("isNaN", {node_d}, boolean());
  auto isnan_d = TreeExprBuilder::MakeFunction("isNaN", {node_e}, boolean());
  auto iszero_d =
      TreeExprBuilder::MakeFunction("equal", {node_e, literal_dn0}, boolean());
  auto process_nan_f =
      TreeExprBuilder::MakeIf(isnan_f, literal_Java_fNaN, node_d, float32());
  auto process_zero_d = TreeExprBuilder::MakeIf(iszero_d, literal_d0, node_e, float64());
  auto process_nan_d =
      TreeExprBuilder::MakeIf(isnan_d, literal_Java_dNaN, process_zero_d, float64());

  auto hash32 = TreeExprBuilder::MakeFunction("hash32", {node_a, literal_10}, int32());
  auto hash32_spark =
      TreeExprBuilder::MakeFunction("hash32_spark", {node_b, literal_0}, int32());
  auto hash64 = TreeExprBuilder::MakeFunction("hash64", {node_a}, int64());
  auto hash64_spark =
      TreeExprBuilder::MakeFunction("hash64_spark", {node_c, literal_0}, int32());
  auto hash32_spark_float =
      TreeExprBuilder::MakeFunction("hash32_spark", {process_nan_f, literal_0}, int32());
  auto hash64_spark_double =
      TreeExprBuilder::MakeFunction("hash64_spark", {process_nan_d, literal_0}, int32());
  auto hash32_spark_bool =
      TreeExprBuilder::MakeFunction("hash32_spark", {node_f, literal_0}, int32());
  auto expr_0 = TreeExprBuilder::MakeExpression(hash32, res_0);
  auto expr_1 = TreeExprBuilder::MakeExpression(hash64, res_1);
  auto expr_2 = TreeExprBuilder::MakeExpression(hash32_spark, res_2);
  auto expr_3 = TreeExprBuilder::MakeExpression(hash64_spark, res_3);
  auto expr_4 = TreeExprBuilder::MakeExpression(hash32_spark_float, res_4);
  auto expr_5 = TreeExprBuilder::MakeExpression(hash64_spark_double, res_5);
  auto expr_6 = TreeExprBuilder::MakeExpression(hash32_spark_bool, res_6);

  // Build a projector for the expression.
  std::shared_ptr<Projector> projector;
  auto status =
      Projector::Make(schema, {expr_0, expr_1, expr_2, expr_3, expr_4, expr_5, expr_6},
                      TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a = MakeArrowArrayInt32({1, 2, 3, 4}, {false, true, true, true});
  auto array_b = MakeArrowArrayInt32({0, -42, 42, 2143289344}, {true, true, true, true});
  auto array_c = MakeArrowArrayInt64({0L, -42L, 42L, 9221120237041090560L},
                                     {true, true, true, true});
  auto array_d = MakeArrowArrayFloat32({706.17, INFINITY, (float)2143289344, NAN},
                                       {true, true, false, true});
  auto array_e = MakeArrowArrayFloat64(
      {706.17, INFINITY, (double)9221120237041090560, -0.0}, {true, true, false, true});
  auto array_f = MakeArrowArrayBool({1, 1, 0, 0}, {false, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(
      schema, num_records, {array_a, array_b, array_c, array_d, array_e, array_f});

  // arrow::PrettyPrint(*in_batch.get(), 2, &std::cout);
  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  auto int32_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(0));
  EXPECT_EQ(int32_arr->null_count(), 0);
  EXPECT_EQ(int32_arr->Value(0), 10);
  for (int i = 1; i < num_records; ++i) {
    EXPECT_NE(int32_arr->Value(i), int32_arr->Value(i - 1));
  }

  auto int64_arr = std::dynamic_pointer_cast<arrow::Int64Array>(outputs.at(1));
  EXPECT_EQ(int64_arr->null_count(), 0);
  EXPECT_EQ(int64_arr->Value(0), 0);
  for (int i = 1; i < num_records; ++i) {
    EXPECT_NE(int64_arr->Value(i), int64_arr->Value(i - 1));
  }

  auto int32_spark_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(2));
  std::vector<int32_t> int32_spark_arr_expect = {593689054, -189366624, -1134849565,
                                                 1927335251};
  for (int i = 0; i < num_records; ++i) {
    EXPECT_EQ(int32_spark_arr->Value(i), int32_spark_arr_expect[i]);
  }

  auto int64_spark_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(3));
  std::vector<int32_t> int64_spark_arr_expect = {1669671676, -846261623, 1871679806,
                                                 1428788237};
  for (int i = 0; i < num_records; ++i) {
    EXPECT_EQ(int64_spark_arr->Value(i), int64_spark_arr_expect[i]);
  }

  auto float32_spark_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(4));
  std::vector<int32_t> float32_spark_arr_expect = {871666867, 1927335251, 0, 1927335251};
  for (int i = 0; i < num_records; ++i) {
    EXPECT_EQ(float32_spark_arr->Value(i), float32_spark_arr_expect[i]);
  }

  auto float64_spark_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(5));
  std::vector<int32_t> float64_spark_arr_expect = {1942731644, 1428788237, 0, 1669671676};
  for (int i = 0; i < num_records; ++i) {
    EXPECT_EQ(float64_spark_arr->Value(i), float64_spark_arr_expect[i]);
  }

  auto bool_spark_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(6));
  std::vector<int32_t> bool_spark_arr_expect = {0, -68075478, 593689054, 593689054};
  for (int i = 0; i < num_records; ++i) {
    EXPECT_EQ(bool_spark_arr->Value(i), bool_spark_arr_expect[i]);
  }
}

TEST_F(TestHash, TestBuf) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto field_b = field("b", utf8());
  auto schema = arrow::schema({field_a, field_b});

  // output fields
  auto res_0 = field("res0", int32());
  auto res_1 = field("res1", int64());
  auto res_2 = field("res2", int32());

  // build expressions.
  // hash32(a)
  // hash64(a, 10)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto literal_10 = TreeExprBuilder::MakeLiteral(static_cast<int64_t>(10));
  auto literal_0 = TreeExprBuilder::MakeLiteral((int32_t)0);
  auto hash32 = TreeExprBuilder::MakeFunction("hash32", {node_a}, int32());
  auto hash64 = TreeExprBuilder::MakeFunction("hash64", {node_a, literal_10}, int64());
  auto hash32_spark =
      TreeExprBuilder::MakeFunction("hashbuf_spark", {node_b, literal_0}, int32());
  auto expr_0 = TreeExprBuilder::MakeExpression(hash32, res_0);
  auto expr_1 = TreeExprBuilder::MakeExpression(hash64, res_1);
  auto expr_2 = TreeExprBuilder::MakeExpression(hash32_spark, res_2);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status =
      Projector::Make(schema, {expr_0, expr_1, expr_2}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a =
      MakeArrowArrayUtf8({"foo", "hello", "bye", "hi"}, {false, true, true, true});
  auto array_b =
      MakeArrowArrayUtf8({"test", "test1", "te", "tes"}, {true, true, true, false});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a, array_b});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  ASSERT_OK(status);

  // Validate results
  auto int32_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(0));
  EXPECT_EQ(int32_arr->null_count(), 0);
  EXPECT_EQ(int32_arr->Value(0), 0);
  for (int i = 1; i < num_records; ++i) {
    EXPECT_NE(int32_arr->Value(i), int32_arr->Value(i - 1));
  }

  auto int64_arr = std::dynamic_pointer_cast<arrow::Int64Array>(outputs.at(1));
  EXPECT_EQ(int64_arr->null_count(), 0);
  EXPECT_EQ(int64_arr->Value(0), 10);
  for (int i = 1; i < num_records; ++i) {
    EXPECT_NE(int64_arr->Value(i), int64_arr->Value(i - 1));
  }

  auto utf8_spark_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(2));
  std::vector<int32_t> utf8_spark_arr_expect = {-1167338989, -1136150618, -2074114216, 0};
  for (int i = 0; i < num_records; ++i) {
    EXPECT_EQ(utf8_spark_arr->Value(i), utf8_spark_arr_expect[i]);
  }
}

TEST_F(TestHash, TestBuf2) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto field_b = field("b", utf8());
  auto schema = arrow::schema({field_a, field_b});

  // output fields
  auto res_0 = field("res0", int32());
  auto res_1 = field("res1", int32());

  // build expressions.
  // hash32(a)
  // hash64(a, 10)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto literal_42 = TreeExprBuilder::MakeLiteral((int32_t)42);
  auto node_seed =
      TreeExprBuilder::MakeFunction("hashbuf_spark", {node_a, literal_42}, int32());
  auto hash32_spark =
      TreeExprBuilder::MakeFunction("hashbuf_spark", {node_b, node_seed}, int32());
  auto expr_0 = TreeExprBuilder::MakeExpression(hash32_spark, res_0);
  auto expr_1 = TreeExprBuilder::MakeExpression(node_seed, res_1);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status =
      Projector::Make(schema, {expr_0, expr_1}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a =
      MakeArrowArrayUtf8({"Williams", "Smith", "", ""}, {true, true, false, false});
  auto array_b =
      MakeArrowArrayUtf8({"Doug", "Kathleen", "", ""}, {true, true, false, false});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a, array_b});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  auto utf8_spark_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(0));
  std::vector<int32_t> utf8_spark_arr_expect = {1506520301, 648517158, 42, 42};
  for (int i = 0; i < num_records; ++i) {
    EXPECT_EQ(utf8_spark_arr->Value(i), utf8_spark_arr_expect[i]);
  }
  auto seed_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(1));
  std::vector<int32_t> seed_arr_expect = {-1535375369, 1628584033, 42, 42};
  for (int i = 0; i < num_records; ++i) {
    EXPECT_EQ(seed_arr->Value(i), seed_arr_expect[i]);
  }
}

TEST_F(TestHash, TestSha256Simple) {
  // schema for input fields
  auto field_a = field("a", int32());
  auto field_b = field("b", int64());
  auto field_c = field("c", float32());
  auto field_d = field("d", float64());
  auto schema = arrow::schema({field_a, field_b, field_c, field_d});

  // output fields
  auto res_0 = field("res0", utf8());
  auto res_1 = field("res1", utf8());
  auto res_2 = field("res2", utf8());
  auto res_3 = field("res3", utf8());

  // build expressions.
  // hashSHA256(a)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto hashSha256_1 = TreeExprBuilder::MakeFunction("hashSHA256", {node_a}, utf8());
  auto expr_0 = TreeExprBuilder::MakeExpression(hashSha256_1, res_0);

  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto hashSha256_2 = TreeExprBuilder::MakeFunction("hashSHA256", {node_b}, utf8());
  auto expr_1 = TreeExprBuilder::MakeExpression(hashSha256_2, res_1);

  auto node_c = TreeExprBuilder::MakeField(field_c);
  auto hashSha256_3 = TreeExprBuilder::MakeFunction("hashSHA256", {node_c}, utf8());
  auto expr_2 = TreeExprBuilder::MakeExpression(hashSha256_3, res_2);

  auto node_d = TreeExprBuilder::MakeField(field_d);
  auto hashSha256_4 = TreeExprBuilder::MakeFunction("hashSHA256", {node_d}, utf8());
  auto expr_3 = TreeExprBuilder::MakeExpression(hashSha256_4, res_3);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_0, expr_1, expr_2, expr_3},
                                TestConfiguration(), &projector);
  ASSERT_OK(status) << status.message();

  // Create a row-batch with some sample data
  int num_records = 2;
  auto validity_array = {false, true};

  auto array_int32 = MakeArrowArrayInt32({1, 0}, validity_array);

  auto array_int64 = MakeArrowArrayInt64({1, 0}, validity_array);

  auto array_float32 = MakeArrowArrayFloat32({1.0, 0.0}, validity_array);

  auto array_float64 = MakeArrowArrayFloat64({1.0, 0.0}, validity_array);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(
      schema, num_records, {array_int32, array_int64, array_float32, array_float64});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  ASSERT_OK(status);

  auto response_int32 = outputs.at(0);
  auto response_int64 = outputs.at(1);
  auto response_float32 = outputs.at(2);
  auto response_float64 = outputs.at(3);

  // Checks if the null and zero representation for numeric values
  // are consistent between the types
  EXPECT_ARROW_ARRAY_EQUALS(response_int32, response_int64);
  EXPECT_ARROW_ARRAY_EQUALS(response_int64, response_float32);
  EXPECT_ARROW_ARRAY_EQUALS(response_float32, response_float64);

  const int sha256_hash_size = 64;

  // Checks if the hash size in response is correct
  for (int i = 1; i < num_records; ++i) {
    const auto& value_at_position = response_int32->GetScalar(i).ValueOrDie()->ToString();

    EXPECT_EQ(value_at_position.size(), sha256_hash_size);
    EXPECT_NE(value_at_position,
              response_int32->GetScalar(i - 1).ValueOrDie()->ToString());
  }
}

TEST_F(TestHash, TestSha256Varlen) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_0 = field("res0", utf8());

  // build expressions.
  // hashSHA256(a)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto hashSha256 = TreeExprBuilder::MakeFunction("hashSHA256", {node_a}, utf8());
  auto expr_0 = TreeExprBuilder::MakeExpression(hashSha256, res_0);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_0}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 3;

  std::string first_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃn\nY "
      "[ˈʏpsilɔn], Yen [jɛn], Yoga [ˈjoːgɑ]";
  std::string second_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeın\nY "
      "[ˈʏpsilɔn], Yen [jɛn], Yoga [ˈjoːgɑ] コンニチハ";

  auto array_a =
      MakeArrowArrayUtf8({"foo", first_string, second_string}, {false, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  ASSERT_OK(status);

  auto response = outputs.at(0);
  const int sha256_hash_size = 64;

  EXPECT_EQ(response->null_count(), 0);

  // Checks that the null value was hashed
  EXPECT_NE(response->GetScalar(0).ValueOrDie()->ToString(), "");
  EXPECT_EQ(response->GetScalar(0).ValueOrDie()->ToString().size(), sha256_hash_size);

  // Check that all generated hashes were different
  for (int i = 1; i < num_records; ++i) {
    const auto& value_at_position = response->GetScalar(i).ValueOrDie()->ToString();

    EXPECT_EQ(value_at_position.size(), sha256_hash_size);
    EXPECT_NE(value_at_position, response->GetScalar(i - 1).ValueOrDie()->ToString());
  }
}

TEST_F(TestHash, TestSha1Simple) {
  // schema for input fields
  auto field_a = field("a", int32());
  auto field_b = field("b", int64());
  auto field_c = field("c", float32());
  auto field_d = field("d", float64());
  auto schema = arrow::schema({field_a, field_b, field_c, field_d});

  // output fields
  auto res_0 = field("res0", utf8());
  auto res_1 = field("res1", utf8());
  auto res_2 = field("res2", utf8());
  auto res_3 = field("res3", utf8());

  // build expressions.
  // hashSHA1(a)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto hashSha1_1 = TreeExprBuilder::MakeFunction("hashSHA1", {node_a}, utf8());
  auto expr_0 = TreeExprBuilder::MakeExpression(hashSha1_1, res_0);

  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto hashSha1_2 = TreeExprBuilder::MakeFunction("hashSHA1", {node_b}, utf8());
  auto expr_1 = TreeExprBuilder::MakeExpression(hashSha1_2, res_1);

  auto node_c = TreeExprBuilder::MakeField(field_c);
  auto hashSha1_3 = TreeExprBuilder::MakeFunction("hashSHA1", {node_c}, utf8());
  auto expr_2 = TreeExprBuilder::MakeExpression(hashSha1_3, res_2);

  auto node_d = TreeExprBuilder::MakeField(field_d);
  auto hashSha1_4 = TreeExprBuilder::MakeFunction("hashSHA1", {node_d}, utf8());
  auto expr_3 = TreeExprBuilder::MakeExpression(hashSha1_4, res_3);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_0, expr_1, expr_2, expr_3},
                                TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 2;
  auto validity_array = {false, true};

  auto array_int32 = MakeArrowArrayInt32({1, 0}, validity_array);

  auto array_int64 = MakeArrowArrayInt64({1, 0}, validity_array);

  auto array_float32 = MakeArrowArrayFloat32({1.0, 0.0}, validity_array);

  auto array_float64 = MakeArrowArrayFloat64({1.0, 0.0}, validity_array);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(
      schema, num_records, {array_int32, array_int64, array_float32, array_float64});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  ASSERT_OK(status);

  auto response_int32 = outputs.at(0);
  auto response_int64 = outputs.at(1);
  auto response_float32 = outputs.at(2);
  auto response_float64 = outputs.at(3);

  // Checks if the null and zero representation for numeric values
  // are consistent between the types
  EXPECT_ARROW_ARRAY_EQUALS(response_int32, response_int64);
  EXPECT_ARROW_ARRAY_EQUALS(response_int64, response_float32);
  EXPECT_ARROW_ARRAY_EQUALS(response_float32, response_float64);

  const int sha1_hash_size = 40;

  // Checks if the hash size in response is correct
  for (int i = 1; i < num_records; ++i) {
    const auto& value_at_position = response_int32->GetScalar(i).ValueOrDie()->ToString();

    EXPECT_EQ(value_at_position.size(), sha1_hash_size);
    EXPECT_NE(value_at_position,
              response_int32->GetScalar(i - 1).ValueOrDie()->ToString());
  }
}

TEST_F(TestHash, TestSha1Varlen) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_0 = field("res0", utf8());

  // build expressions.
  // hashSHA1(a)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto hashSha1 = TreeExprBuilder::MakeFunction("hashSHA1", {node_a}, utf8());
  auto expr_0 = TreeExprBuilder::MakeExpression(hashSha1, res_0);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_0}, TestConfiguration(), &projector);
  ASSERT_OK(status) << status.message();

  // Create a row-batch with some sample data
  int num_records = 3;

  std::string first_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃn\nY [ˈʏpsilɔn], "
      "Yen [jɛn], Yoga [ˈjoːgɑ]";
  std::string second_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeın\nY [ˈʏpsilɔn], "
      "Yen [jɛn], Yoga [ˈjoːgɑ] コンニチハ";

  auto array_a =
      MakeArrowArrayUtf8({"", first_string, second_string}, {false, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  ASSERT_OK(status);

  auto response = outputs.at(0);
  const int sha1_hash_size = 40;

  EXPECT_EQ(response->null_count(), 0);

  // Checks that the null value was hashed
  EXPECT_NE(response->GetScalar(0).ValueOrDie()->ToString(), "");
  EXPECT_EQ(response->GetScalar(0).ValueOrDie()->ToString().size(), sha1_hash_size);

  // Check that all generated hashes were different
  for (int i = 1; i < num_records; ++i) {
    const auto& value_at_position = response->GetScalar(i).ValueOrDie()->ToString();

    EXPECT_EQ(value_at_position.size(), sha1_hash_size);
    EXPECT_NE(value_at_position, response->GetScalar(i - 1).ValueOrDie()->ToString());
  }
}

TEST_F(TestHash, TestSha1FunctionsAlias) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto field_b = field("c", int64());
  auto field_c = field("e", float64());
  auto schema = arrow::schema({field_a, field_b, field_c});

  // output fields
  auto res_0 = field("res0", utf8());
  auto res_0_sha1 = field("res0sha1", utf8());
  auto res_0_sha = field("res0sha", utf8());

  auto res_1 = field("res1", utf8());
  auto res_1_sha1 = field("res1sha1", utf8());
  auto res_1_sha = field("res1sha", utf8());

  auto res_2 = field("res2", utf8());
  auto res_2_sha1 = field("res2_sha1", utf8());
  auto res_2_sha = field("res2_sha", utf8());

  // build expressions.
  // hashSHA1(a)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto hashSha1 = TreeExprBuilder::MakeFunction("hashSHA1", {node_a}, utf8());
  auto expr_0 = TreeExprBuilder::MakeExpression(hashSha1, res_0);
  auto sha1 = TreeExprBuilder::MakeFunction("sha1", {node_a}, utf8());
  auto expr_0_sha1 = TreeExprBuilder::MakeExpression(sha1, res_0_sha1);
  auto sha = TreeExprBuilder::MakeFunction("sha", {node_a}, utf8());
  auto expr_0_sha = TreeExprBuilder::MakeExpression(sha, res_0_sha);

  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto hashSha1_1 = TreeExprBuilder::MakeFunction("hashSHA1", {node_b}, utf8());
  auto expr_1 = TreeExprBuilder::MakeExpression(hashSha1_1, res_1);
  auto sha1_1 = TreeExprBuilder::MakeFunction("sha1", {node_b}, utf8());
  auto expr_1_sha1 = TreeExprBuilder::MakeExpression(sha1_1, res_1_sha1);
  auto sha_1 = TreeExprBuilder::MakeFunction("sha", {node_b}, utf8());
  auto expr_1_sha = TreeExprBuilder::MakeExpression(sha_1, res_1_sha);

  auto node_c = TreeExprBuilder::MakeField(field_c);
  auto hashSha1_2 = TreeExprBuilder::MakeFunction("hashSHA1", {node_c}, utf8());
  auto expr_2 = TreeExprBuilder::MakeExpression(hashSha1_2, res_2);
  auto sha1_2 = TreeExprBuilder::MakeFunction("sha1", {node_c}, utf8());
  auto expr_2_sha1 = TreeExprBuilder::MakeExpression(sha1_2, res_2_sha1);
  auto sha_2 = TreeExprBuilder::MakeFunction("sha", {node_c}, utf8());
  auto expr_2_sha = TreeExprBuilder::MakeExpression(sha_2, res_2_sha);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema,
                                {expr_0, expr_0_sha, expr_0_sha1, expr_1, expr_1_sha,
                                 expr_1_sha1, expr_2, expr_2_sha, expr_2_sha1},
                                TestConfiguration(), &projector);
  ASSERT_OK(status) << status.message();

  // Create a row-batch with some sample data
  int32_t num_records = 3;

  std::string first_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃn\nY [ˈʏpsilɔn], "
      "Yen [jɛn], Yoga [ˈjoːgɑ]";
  std::string second_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeın\nY [ˈʏpsilɔn], "
      "Yen [jɛn], Yoga [ˈjoːgɑ] コンニチハ";

  auto array_utf8 =
      MakeArrowArrayUtf8({"", first_string, second_string}, {false, true, true});

  auto validity_array = {false, true, true};

  auto array_int64 = MakeArrowArrayInt64({1, 0, 32423}, validity_array);

  auto array_float64 = MakeArrowArrayFloat64({1.0, 0.0, 324893.3849}, validity_array);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records,
                                           {array_utf8, array_int64, array_float64});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  ASSERT_OK(status);

  // Checks that the response for the hashSHA1, sha and sha1 are equals for the first
  // field of utf8 type
  EXPECT_ARROW_ARRAY_EQUALS(outputs.at(0), outputs.at(1));  // hashSha1 and sha
  EXPECT_ARROW_ARRAY_EQUALS(outputs.at(1), outputs.at(2));  // sha and sha1

  // Checks that the response for the hashSHA1, sha and sha1 are equals for the second
  // field of int64 type
  EXPECT_ARROW_ARRAY_EQUALS(outputs.at(3), outputs.at(4));  // hashSha1 and sha
  EXPECT_ARROW_ARRAY_EQUALS(outputs.at(4), outputs.at(5));  // sha and sha1

  // Checks that the response for the hashSHA1, sha and sha1 are equals for the first
  // field of float64 type
  EXPECT_ARROW_ARRAY_EQUALS(outputs.at(6), outputs.at(7));  // hashSha1 and sha responses
  EXPECT_ARROW_ARRAY_EQUALS(outputs.at(7), outputs.at(8));  // sha and sha1 responses
}

TEST_F(TestHash, TestSha256FunctionsAlias) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto field_b = field("c", int64());
  auto field_c = field("e", float64());
  auto schema = arrow::schema({field_a, field_b, field_c});

  // output fields
  auto res_0 = field("res0", utf8());
  auto res_0_sha256 = field("res0sha256", utf8());

  auto res_1 = field("res1", utf8());
  auto res_1_sha256 = field("res1sha256", utf8());

  auto res_2 = field("res2", utf8());
  auto res_2_sha256 = field("res2_sha256", utf8());

  // build expressions.
  // hashSHA1(a)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto hashSha2 = TreeExprBuilder::MakeFunction("hashSHA256", {node_a}, utf8());
  auto expr_0 = TreeExprBuilder::MakeExpression(hashSha2, res_0);
  auto sha256 = TreeExprBuilder::MakeFunction("sha256", {node_a}, utf8());
  auto expr_0_sha256 = TreeExprBuilder::MakeExpression(sha256, res_0_sha256);

  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto hashSha2_1 = TreeExprBuilder::MakeFunction("hashSHA256", {node_b}, utf8());
  auto expr_1 = TreeExprBuilder::MakeExpression(hashSha2_1, res_1);
  auto sha256_1 = TreeExprBuilder::MakeFunction("sha256", {node_b}, utf8());
  auto expr_1_sha256 = TreeExprBuilder::MakeExpression(sha256_1, res_1_sha256);

  auto node_c = TreeExprBuilder::MakeField(field_c);
  auto hashSha2_2 = TreeExprBuilder::MakeFunction("hashSHA256", {node_c}, utf8());
  auto expr_2 = TreeExprBuilder::MakeExpression(hashSha2_2, res_2);
  auto sha256_2 = TreeExprBuilder::MakeFunction("sha256", {node_c}, utf8());
  auto expr_2_sha256 = TreeExprBuilder::MakeExpression(sha256_2, res_2_sha256);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(
      schema, {expr_0, expr_0_sha256, expr_1, expr_1_sha256, expr_2, expr_2_sha256},
      TestConfiguration(), &projector);
  ASSERT_OK(status) << status.message();

  // Create a row-batch with some sample data
  int32_t num_records = 3;

  std::string first_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃn\nY [ˈʏpsilɔn], "
      "Yen [jɛn], Yoga [ˈjoːgɑ]";
  std::string second_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeın\nY [ˈʏpsilɔn], "
      "Yen [jɛn], Yoga [ˈjoːgɑ] コンニチハ";

  auto array_utf8 =
      MakeArrowArrayUtf8({"", first_string, second_string}, {false, true, true});

  auto validity_array = {false, true, true};

  auto array_int64 = MakeArrowArrayInt64({1, 0, 32423}, validity_array);

  auto array_float64 = MakeArrowArrayFloat64({1.0, 0.0, 324893.3849}, validity_array);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records,
                                           {array_utf8, array_int64, array_float64});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  ASSERT_OK(status);

  // Checks that the response for the hashSHA2, sha256 and sha2 are equals for the first
  // field of utf8 type
  EXPECT_ARROW_ARRAY_EQUALS(outputs.at(0), outputs.at(1));  // hashSha2 and sha256

  // Checks that the response for the hashSHA2, sha256 and sha2 are equals for the second
  // field of int64 type
  EXPECT_ARROW_ARRAY_EQUALS(outputs.at(2), outputs.at(3));  // hashSha2 and sha256

  // Checks that the response for the hashSHA2, sha256 and sha2 are equals for the first
  // field of float64 type
  EXPECT_ARROW_ARRAY_EQUALS(outputs.at(4),
                            outputs.at(5));  // hashSha2 and sha256 responses
}

TEST_F(TestHash, TestMD5Simple) {
  // schema for input fields
  auto field_a = field("a", int32());
  auto field_b = field("b", int64());
  auto field_c = field("c", float32());
  auto field_d = field("d", float64());
  auto schema = arrow::schema({field_a, field_b, field_c, field_d});

  // output fields
  auto res_0 = field("res0", utf8());
  auto res_1 = field("res1", utf8());
  auto res_2 = field("res2", utf8());
  auto res_3 = field("res3", utf8());

  // build expressions.
  // hashMD5(a)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto hashMD5_1 = TreeExprBuilder::MakeFunction("hashMD5", {node_a}, utf8());
  auto expr_0 = TreeExprBuilder::MakeExpression(hashMD5_1, res_0);

  auto node_b = TreeExprBuilder::MakeField(field_b);
  auto hashMD5_2 = TreeExprBuilder::MakeFunction("hashMD5", {node_b}, utf8());
  auto expr_1 = TreeExprBuilder::MakeExpression(hashMD5_2, res_1);

  auto node_c = TreeExprBuilder::MakeField(field_c);
  auto hashMD5_3 = TreeExprBuilder::MakeFunction("hashMD5", {node_c}, utf8());
  auto expr_2 = TreeExprBuilder::MakeExpression(hashMD5_3, res_2);

  auto node_d = TreeExprBuilder::MakeField(field_d);
  auto hashMD5_4 = TreeExprBuilder::MakeFunction("hashMD5", {node_d}, utf8());
  auto expr_3 = TreeExprBuilder::MakeExpression(hashMD5_4, res_3);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_0, expr_1, expr_2, expr_3},
                                TestConfiguration(), &projector);
  ASSERT_OK(status) << status.message();

  // Create a row-batch with some sample data
  int num_records = 2;
  auto validity_array = {false, true};

  auto array_int32 = MakeArrowArrayInt32({1, 0}, validity_array);

  auto array_int64 = MakeArrowArrayInt64({1, 0}, validity_array);

  auto array_float32 = MakeArrowArrayFloat32({1.0, 0.0}, validity_array);

  auto array_float64 = MakeArrowArrayFloat64({1.0, 0.0}, validity_array);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(
      schema, num_records, {array_int32, array_int64, array_float32, array_float64});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  ASSERT_OK(status);

  auto response_int32 = outputs.at(0);
  auto response_int64 = outputs.at(1);
  auto response_float32 = outputs.at(2);
  auto response_float64 = outputs.at(3);

  // Checks if the null and zero representation for numeric values
  // are consistent between the types
  EXPECT_ARROW_ARRAY_EQUALS(response_int32, response_int64);
  EXPECT_ARROW_ARRAY_EQUALS(response_int64, response_float32);
  EXPECT_ARROW_ARRAY_EQUALS(response_float32, response_float64);

  const int MD5_hash_size = 32;

  // Checks if the hash size in response is correct
  for (int i = 1; i < num_records; ++i) {
    const auto& value_at_position = response_int32->GetScalar(i).ValueOrDie()->ToString();

    EXPECT_EQ(value_at_position.size(), MD5_hash_size);
    EXPECT_NE(value_at_position,
              response_int32->GetScalar(i - 1).ValueOrDie()->ToString());
  }
}

TEST_F(TestHash, TestMD5Varlen) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_0 = field("res0", utf8());

  // build expressions.
  // hashMD5(a)
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto hashMD5 = TreeExprBuilder::MakeFunction("hashMD5", {node_a}, utf8());
  auto expr_0 = TreeExprBuilder::MakeExpression(hashMD5, res_0);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_0}, TestConfiguration(), &projector);
  ASSERT_OK(status) << status.message();

  // Create a row-batch with some sample data
  int num_records = 3;

  std::string first_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃn\nY [ˈʏpsilɔn], "
      "Yen [jɛn], Yoga [ˈjoːgɑ]";
  std::string second_string =
      "ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeın\nY [ˈʏpsilɔn], "
      "Yen [jɛn], Yoga [ˈjoːgɑ] コンニチハ";

  auto array_a =
      MakeArrowArrayUtf8({"", first_string, second_string}, {false, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  ASSERT_OK(status);

  auto response = outputs.at(0);
  const int MD5_hash_size = 32;

  EXPECT_EQ(response->null_count(), 0);

  // Checks that the null value was hashed
  EXPECT_NE(response->GetScalar(0).ValueOrDie()->ToString(), "");
  EXPECT_EQ(response->GetScalar(0).ValueOrDie()->ToString().size(), MD5_hash_size);

  // Check that all generated hashes were different
  for (int i = 1; i < num_records; ++i) {
    const auto& value_at_position = response->GetScalar(i).ValueOrDie()->ToString();

    EXPECT_EQ(value_at_position.size(), MD5_hash_size);
    EXPECT_NE(value_at_position, response->GetScalar(i - 1).ValueOrDie()->ToString());
  }
}
}  // namespace gandiva
