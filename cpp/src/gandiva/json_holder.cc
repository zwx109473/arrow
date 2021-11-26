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

#include <iostream>
#include <regex>

#include "gandiva/node.h"
#include "gandiva/regex_util.h"

namespace gandiva {

Status JsonHolder::Make(const FunctionNode& node, std::shared_ptr<JsonHolder>* holder) {
  return Make(holder);
}

Status JsonHolder::Make(std::shared_ptr<JsonHolder>* holder) {
  *holder = std::shared_ptr<JsonHolder>(new JsonHolder());
  return Status::OK();
}

const uint8_t* JsonHolder::operator()(const std::string& json_str, const std::string& json_path, int32_t* out_len) {
 
  std::unique_ptr<arrow::json::BlockParser> parser;
  (arrow::json::BlockParser::Make(parse_options_, &parser));

  (parser->Parse(std::make_shared<arrow::Buffer>(json_str)));
  std::shared_ptr<arrow::Array> parsed;
  (parser->Finish(&parsed));
  auto struct_parsed = std::dynamic_pointer_cast<arrow::StructArray>(parsed);

  //json_path example: $.col_14, will extract col_14 here
  // needs to gurad failure here
  auto col_name = json_path.substr(2);

  auto dict_parsed = std::dynamic_pointer_cast<arrow::DictionaryArray>(
      struct_parsed->GetFieldByName(col_name));
  auto dict_array = dict_parsed->dictionary();
  auto uft8_array = std::dynamic_pointer_cast<arrow::BinaryArray>(dict_array);
  
  return uft8_array->GetValue(0, out_len);
}

}  // namespace gandiva
