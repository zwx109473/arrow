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

#include "gandiva/json_holder.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "gandiva/regex_util.h"

namespace gandiva {

class TestJsonHolder : public ::testing::Test {};

TEST_F(TestJsonHolder, TestJson) {
  std::shared_ptr<JsonHolder> json_holder;

  auto status = JsonHolder::Make(&json_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& get_json_object = *json_holder;

  int32_t out_len;
  const uint8_t* data = get_json_object(R"({"hello": 3.5 })", "$.hello", &out_len);
  EXPECT_EQ(std::string((char*)data, out_len), "3.5");

}

}  // namespace gandiva
