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

#include "gandiva/substr_index_holder.h"

#include <gtest/gtest.h>

namespace gandiva {
class TestSubstrIndexHolder : public ::testing::Test {
 protected:
  ExecutionContext ctx_;
};

TEST_F(TestSubstrIndexHolder, TestSubstrIndex) {
  std::shared_ptr<SubstrIndexHolder> substr_index_holder;

  auto status = SubstrIndexHolder::Make(&substr_index_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto substr_index = *substr_index_holder;
  int32_t out_len = 0;
  const char* out_str;
  auto ctx_ptr = &ctx_;

  // Count = 0
  out_str = substr_index(ctx_ptr, "www.apache.org", ".", 0, &out_len);
  EXPECT_EQ(out_len, 0);
  EXPECT_EQ(std::string(out_str, out_len), "");

  // Count = 1
  out_str = substr_index(ctx_ptr, "www.apache.org", ".", 1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "www");

  // Count = 2
  out_str = substr_index(ctx_ptr, "www.apache.org", ".", 2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "www.apache");

  // Boundary case.
  out_str = substr_index(ctx_ptr, "www.apache.org", "www", 1, &out_len);
  EXPECT_EQ(out_len, 0);
  EXPECT_EQ(std::string(out_str, out_len), "");

  // The actual delimiter count in a string is less than the specified count.
  out_str = substr_index(ctx_ptr, "www.apache.org", ".", 3, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "www.apache.org");

  // Negative count.
  out_str = substr_index(ctx_ptr, "www.apache.org", ".", -1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "org");

  // Negative count.
  out_str = substr_index(ctx_ptr, "www.apache.org", ".", -2, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), "apache.org");

  // Delimiter length > 1 for negative count case.
  out_str = substr_index(ctx_ptr, "www.apache.org", "apache", -1, &out_len);
  EXPECT_EQ(std::string(out_str, out_len), ".org");

  // Boundary case for negative count.
  out_str = substr_index(ctx_ptr, "www.apache.org", "org", -1, &out_len);
  EXPECT_EQ(out_len, 0);
  EXPECT_EQ(std::string(out_str, out_len), "");
}

} // namespace gandiva