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

#include <memory>
#include <string>

#include "arrow/status.h"
#include "gandiva/execution_context.h"
#include "gandiva/function_holder.h"
#include "gandiva/node.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// Function Holder for SQL 'substring_index'
class GANDIVA_EXPORT SubstrIndexHolder : public FunctionHolder {
 public:
  SubstrIndexHolder() {}
  ~SubstrIndexHolder() override = default;

  static Status Make(const FunctionNode& node, std::shared_ptr<SubstrIndexHolder>* holder);
  static Status Make(std::shared_ptr<SubstrIndexHolder>* holder);

  const char* operator()(gandiva::ExecutionContext* ctx, std::string input_str,
                            std::string delim_str, int count, int32_t* out_len);
};

}  // namespace gandiva