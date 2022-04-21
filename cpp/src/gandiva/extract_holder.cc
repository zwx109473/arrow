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

#include "gandiva/extract_holder.h"

#include "gandiva/node.h"
#include "gandiva/regex_util.h"

namespace gandiva {

  static bool IsArrowStringLiteral(arrow::Type::type type) {
    return type == arrow::Type::STRING || type == arrow::Type::BINARY;
  }

  Status ExtractHolder::Make(const FunctionNode& node, std::shared_ptr<ExtractHolder>* holder) {
    ARROW_RETURN_IF(node.children().size() != 3,
                    Status::Invalid("'extract' function requires three parameters"));
    auto pattern_node = dynamic_cast<LiteralNode*>(node.children().at(1).get());
    ARROW_RETURN_IF(
        pattern_node == nullptr,
        Status::Invalid("'extract' function requires a literal as the second parameter"));
    auto pattern_node_type = pattern_node->return_type()->id();
    ARROW_RETURN_IF(!IsArrowStringLiteral(pattern_node_type), Status::Invalid(
            "'extract' function requires a string literal as the second parameter"));

    std::string pattern = arrow::util::get<std::string>(pattern_node->holder());
    auto index_node = dynamic_cast<LiteralNode*>(node.children().at(2).get());
    auto index = arrow::util::get<int32_t>(index_node->holder());
    // If regex group index is 0, which means matching the entire regular expression,
    // wrap the pattern with "()".
    if (index == 0) {
      pattern = "(" + pattern + ")";
    }
    return Make(pattern, holder);
  }

  Status ExtractHolder::Make(const std::string& sql_pattern,
                             std::shared_ptr<ExtractHolder>* holder) {
    auto extractHolder = std::shared_ptr<ExtractHolder>(new ExtractHolder(sql_pattern));
    ARROW_RETURN_IF(!extractHolder->re2_.ok(),
                    Status::Invalid("Building RE2 pattern '", sql_pattern, "' failed"));
    *holder = extractHolder;
    return Status::OK();
  }

  void ExtractHolder::return_error(ExecutionContext* context,
                                   const std::string& data, const std::string& reason) {
    std::string err_msg = "Error in extracting the string on the given string '" +
                          data + "' for the given pattern: " + pattern_ + ", caused by " + reason;
    context->set_error_msg(err_msg.c_str());
  }

}  // namespace gandiva
