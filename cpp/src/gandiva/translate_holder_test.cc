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

#include "gandiva/translate_holder.h"

#include <gtest/gtest.h>

namespace gandiva {
class TestTranslateHolder : public ::testing::Test {
 protected:
  ExecutionContext ctx_;
};

TEST_F(TestTranslateHolder, TestTranslate) {
  std::shared_ptr<TranslateHolder> translate_holder;

  auto status = TranslateHolder::Make(&translate_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto translate = *translate_holder;

  int32_t out_len;
  const uint8_t* out_str;

  out_str = translate(&ctx_, "ab[cd]", "[]", "", &out_len);
  EXPECT_EQ(std::string((char*)out_str, out_len), "abcd");

  out_str = translate(&ctx_, "ab[cd]", "[]", "#", &out_len);
  EXPECT_EQ(std::string((char*)out_str, out_len), "ab#cd");
}

} // namespace gandiva