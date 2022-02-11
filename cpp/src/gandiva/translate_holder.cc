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

#include <unordered_map>
#include "gandiva/node.h"

namespace gandiva {

Status TranslateHolder::Make(const FunctionNode& node, std::shared_ptr<TranslateHolder>* holder) {
  return Make(holder);
}

Status TranslateHolder::Make(std::shared_ptr<TranslateHolder>* holder) {
  *holder = std::shared_ptr<TranslateHolder>(new TranslateHolder());
  return Status::OK();
}

const uint8_t* TranslateHolder::operator()(gandiva::ExecutionContext* ctx, std::string text, 
                          std::string matching_str, std::string replace_str, int32_t* out_len) {
  char res[text.length()];
  std::unordered_map<char, char> replace_map;
  for (uint64_t i = 0; i < matching_str.length(); i++) {
    if (i >= replace_str.length()) {
      replace_map[matching_str[i]] = '\0';
    } else {
      replace_map[matching_str[i]] = replace_str[i];
    }
  }
  int j = 0;
  for (uint64_t i = 0; i < text.length(); i++) {
    if (replace_map.find(text[i]) == replace_map.end()) {
      res[j++] = text[i];
      continue;
    }
    char replace_char = replace_map[text[i]];
    if (replace_char != '\0') {
      res[j++] = replace_char;
    }
  }
  *out_len = j;
  auto result_buffer = reinterpret_cast<uint8_t*>(ctx->arena()->Allocate(*out_len));
  memcpy(result_buffer, std::string((char*)res, *out_len).data(), *out_len);
  return result_buffer;
}

} // namespace gandiva