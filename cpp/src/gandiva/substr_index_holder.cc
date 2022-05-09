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

#include <string>
#include "gandiva/node.h"

namespace gandiva {

Status SubstrIndexHolder::Make(const FunctionNode& node, std::shared_ptr<SubstrIndexHolder>* holder) {
  return Make(holder);
}

Status SubstrIndexHolder::Make(std::shared_ptr<SubstrIndexHolder>* holder) {
  *holder = std::shared_ptr<SubstrIndexHolder>(new SubstrIndexHolder());
  return Status::OK();
}

const char* SubstrIndexHolder::operator()(gandiva::ExecutionContext* ctx, std::string input_str, 
                          std::string delim, int count, int32_t* out_len) {
  // No need to find delim when count = 0 and just return "".
  if (count == 0) {
    *out_len = 0;
    return "";
  }

  auto in_len = input_str.length();
  auto delim_len = delim.length();
  std::size_t index;
  if (count > 0) {
    int n = 0;
    index = 0;
    while (n++ < count) {
      index = input_str.find(delim, index);
      if (index == std::string::npos) {
        break;
      }
      if (n < count) {
        index++;
      }
    }
  } else {
    int n = 0;
    index = in_len - 1;
    while (n++ < -count) {
      index = input_str.rfind(delim, index);
      if (index == std::string::npos) {
        break;
      }
      if (n < -count) {
        index--;
      }
    }
  }

  if (index == std::string::npos) {
    *out_len = static_cast<int32_t>(input_str.length());
    char* result_buffer = reinterpret_cast<char*>(ctx->arena()->Allocate(*out_len));
    if (result_buffer == NULLPTR) {
      ctx->set_error_msg("Could not allocate memory for result");
      *out_len = 0;
      return "";
    }
    memcpy(result_buffer, input_str.data(), *out_len);
    return result_buffer;
  }

  std::string result;
  if (count > 0) {
    *out_len = static_cast<int32_t>(index);
    result = input_str.substr(0, *out_len);
  } else {
    // Delimiter length can be greater than 1,
    *out_len = static_cast<int32_t>(input_str.length() - index - delim_len);
    result = input_str.substr(index + delim_len, *out_len);
  }

  char* result_buffer = reinterpret_cast<char*>(ctx->arena()->Allocate(*out_len));
  if (result_buffer == NULLPTR) {
    ctx->set_error_msg("Could not allocate memory for result");
    *out_len = 0;
    return "";
  }
  memcpy(result_buffer, result.data(), *out_len);
  return result_buffer;
}

} // namespace gandiva