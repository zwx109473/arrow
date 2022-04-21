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

#pragma once

#include <re2/re2.h>

#include <memory>
#include <string>

#include "arrow/status.h"
#include "gandiva/execution_context.h"
#include "gandiva/function_holder.h"
#include "gandiva/node.h"
#include "gandiva/visibility.h"

namespace gandiva {

  /// Function Holder for 'regexp_extract'
  class GANDIVA_EXPORT ExtractHolder: public FunctionHolder {
  public:
  ~ExtractHolder() override = default;

  // Invoked by function_holder_registry.h
  static Status Make(const FunctionNode &node, std::shared_ptr<ExtractHolder> *holder);

  static Status Make(const std::string& sql_pattern, std::shared_ptr<ExtractHolder>* holder);

  /// Return the substring that matches the given regex. The idx is the group id for the pattern.
  /// Default is 1, which means only return the string that matches the first group. 0 means the
  /// string that matches all groups are returned.
  const char *operator()(ExecutionContext *ctx, const char *user_input,
                         int32_t user_input_len, int32_t idx, int32_t *out_length) {
    std::string user_input_as_str(user_input, user_input_len);
    std::string out;

    int groups_num = re2_.NumberOfCapturingGroups();
    if (idx > groups_num) {
      return_error(ctx, user_input_as_str, "the specified group index exceeds regex group count");
      *out_length = 0;
      return "";
    }
    if (idx < 0) {
      return_error(ctx, user_input_as_str, "the specified group index should NOT be negative!");
      *out_length = 0;
      return "";
    }

    bool matched = false;
    if (idx == 0) {
      matched = RE2::PartialMatch(user_input_as_str, re2_, &out);
    } else {
      RE2::Arg *args[groups_num];
      for (int i = 0; i < groups_num; i++) {
        args[i] = new RE2::Arg;
      }
      *(args[idx - 1]) = &out;
      // Use re2_ instead of pattern_ for better performance.
      matched = RE2::PartialMatchN(user_input_as_str, re2_, args, groups_num);
    }
    if (!matched) {
      *out_length = 0;
      return "";
    }

    *out_length = static_cast<int32_t>(out.length());
    if (*out_length == 0) {
      return "";
    }

    char *result_buffer = reinterpret_cast<char *>(ctx->arena()->Allocate(*out_length));
    if (result_buffer == NULLPTR) {
      ctx->set_error_msg("Could not allocate memory for result! Wrong result may be returned!");
      *out_length = 0;
      return "";
    }
    memcpy(result_buffer, out.data(), *out_length);

    return result_buffer;
  }

  private:

  explicit ExtractHolder(const std::string& pattern): pattern_(pattern), re2_(pattern) {}

  void return_error(ExecutionContext *context, const std::string& data, const std::string& reason);

  std::string pattern_;  // posix pattern string, to help debugging
  RE2 re2_;            // compiled regex for the pattern
};

}  // namespace gandiva
