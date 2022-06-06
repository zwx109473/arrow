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

#include "gandiva/rlike_holder.h"
#include "gandiva/regex_util.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

namespace gandiva {

class TestRLikeHolder : public ::testing::Test {
 public:
  RE2::Options regex_op;
  FunctionNode BuildLike(std::string pattern) {
    auto field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
    auto pattern_node =
        std::make_shared<LiteralNode>(arrow::utf8(), LiteralHolder(pattern), false);
    return FunctionNode("rlike", {field, pattern_node}, arrow::boolean());
  }
};

TEST_F(TestRLikeHolder, TestMatchAny) {
  std::shared_ptr<RLikeHolder> like_holder;

  auto status = RLikeHolder::Make("ab*", &like_holder, regex_op);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;
  EXPECT_TRUE(like("ab"));
  EXPECT_TRUE(like("abc"));
  EXPECT_TRUE(like("abcd"));
  EXPECT_TRUE(like("a"));
  EXPECT_TRUE(like("cab"));

  EXPECT_FALSE(like("cd"));

  status = RLikeHolder::Make("13.(2|3|4)", &like_holder, regex_op);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like2 = *like_holder;
  EXPECT_TRUE(like2("13.3.0.3"));
  EXPECT_TRUE(like2("13.2.0.10"));

  EXPECT_FALSE(like2("13.1.0.10"));
}

TEST_F(TestRLikeHolder, TestDot) {
  std::shared_ptr<RLikeHolder> like_holder;

  auto status = RLikeHolder::Make("abc.", &like_holder, regex_op);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& like = *like_holder;
  EXPECT_TRUE(like("abcd"));
}

}  // namespace gandiva
