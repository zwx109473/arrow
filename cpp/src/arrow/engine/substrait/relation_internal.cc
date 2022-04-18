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

#include "arrow/engine/substrait/relation_internal.h"

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/exec/options.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/plan.h"
#include "arrow/dataset/scanner.h"
#include "arrow/engine/substrait/expression_internal.h"
#include "arrow/engine/substrait/type_internal.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace engine {

template <typename RelMessage>
Status CheckRelCommon(const RelMessage& rel) {
  if (rel.has_common()) {
    if (rel.common().has_emit()) {
      return Status::NotImplemented("substrait::RelCommon::Emit");
    }
    if (rel.common().has_hint()) {
      return Status::NotImplemented("substrait::RelCommon::Hint");
    }
    if (rel.common().has_advanced_extension()) {
      return Status::NotImplemented("substrait::RelCommon::advanced_extension");
    }
  }
  if (rel.has_advanced_extension()) {
    return Status::NotImplemented("substrait AdvancedExtensions");
  }
  return Status::OK();
}

Result<compute::Declaration> FromProto(const substrait::Rel& rel,
                                       const ExtensionSet& ext_set) {
  static bool dataset_init = false;
  if (!dataset_init) {
    dataset_init = true;
    dataset::internal::Initialize();
  }

  switch (rel.rel_type_case()) {
    case substrait::Rel::RelTypeCase::kRead: {
      const auto& read = rel.read();
      RETURN_NOT_OK(CheckRelCommon(read));

      ARROW_ASSIGN_OR_RAISE(auto base_schema, FromProto(read.base_schema(), ext_set));

      auto scan_options = std::make_shared<dataset::ScanOptions>();

      // FIXME: FieldPath is not supported in scan filter. See ARROW-14658
      if (read.has_filter()) {
        // ARROW_ASSIGN_OR_RAISE(scan_options->filter, FromProto(read.filter(), ext_set));
      }

      if (read.has_projection()) {
        // NOTE: scan_options->projection is not used by the scanner and thus can't be
        // used for this
        return Status::NotImplemented("substrait::ReadRel::projection");
      }

      if (!read.has_local_files()) {
        return Status::NotImplemented(
            "substrait::ReadRel with read_type other than LocalFiles");
      }

      if (read.local_files().has_advanced_extension()) {
        return Status::NotImplemented(
            "substrait::ReadRel::LocalFiles::advanced_extension");
      }

      auto head = read.local_files().items().at(0);
      if (head.path_type_case() == substrait::ReadRel_LocalFiles_FileOrFiles::kUriFile &&
          util::string_view{head.uri_file()}.starts_with("iterator:")) {
        const auto& index = head.uri_file().substr(9);
        return compute::Declaration{"source_index",
                                    compute::SourceIndexOptions{std::stoi(index)}};
      }

      ARROW_ASSIGN_OR_RAISE(auto filesystem, fs::FileSystemFromUri(head.uri_file()));
      auto format = std::make_shared<dataset::ParquetFileFormat>();
      std::vector<std::shared_ptr<dataset::FileFragment>> fragments;

      for (const auto& item : read.local_files().items()) {
        if (item.path_type_case() !=
            substrait::ReadRel_LocalFiles_FileOrFiles::kUriFile) {
          return Status::NotImplemented(
              "substrait::ReadRel::LocalFiles::FileOrFiles with "
              "path_type other than uri_file");
        }

        if (item.format() !=
            substrait::ReadRel::LocalFiles::FileOrFiles::FILE_FORMAT_PARQUET) {
          return Status::NotImplemented(
              "substrait::ReadRel::LocalFiles::FileOrFiles::format "
              "other than FILE_FORMAT_PARQUET");
        }

        auto uri = arrow::internal::Uri();
        RETURN_NOT_OK(uri.Parse(item.uri_file()));
        auto path = uri.path();

        // TODO: partition index.
        if (item.partition_index() != 0) {
          //          return Status::NotImplemented(
          //              "non-default
          //              substrait::ReadRel::LocalFiles::FileOrFiles::partition_index");
        }

        // Read all row groups if both start and length are not specified.
        int64_t start_offset = item.length() == 0 && item.start() == 0
                                   ? -1
                                   : static_cast<int64_t>(item.start());
        int64_t length = static_cast<int64_t>(item.length());

        ARROW_ASSIGN_OR_RAISE(auto fragment,
                              format->MakeFragment(dataset::FileSource{
                                  std::move(path), filesystem, start_offset, length}));
        fragments.push_back(std::move(fragment));
      }

      ARROW_ASSIGN_OR_RAISE(
          auto ds, dataset::FileSystemDataset::Make(
                       std::move(base_schema), /*root_partition=*/compute::literal(true),
                       std::move(format), std::move(filesystem), std::move(fragments)));

      return compute::Declaration{
          "scan", dataset::ScanNodeOptions{std::move(ds), std::move(scan_options)}};
    }

    case substrait::Rel::RelTypeCase::kFilter: {
      const auto& filter = rel.filter();
      RETURN_NOT_OK(CheckRelCommon(filter));

      if (!filter.has_input()) {
        return Status::Invalid("substrait::FilterRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input, FromProto(filter.input(), ext_set));

      if (!filter.has_condition()) {
        return Status::Invalid("substrait::FilterRel with no condition expression");
      }
      ARROW_ASSIGN_OR_RAISE(auto condition, FromProto(filter.condition(), ext_set));

      return compute::Declaration::Sequence({
          std::move(input),
          {"filter", compute::FilterNodeOptions{std::move(condition)}},
      });
    }

    case substrait::Rel::RelTypeCase::kProject: {
      const auto& project = rel.project();
      RETURN_NOT_OK(CheckRelCommon(project));

      if (!project.has_input()) {
        return Status::Invalid("substrait::ProjectRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input, FromProto(project.input(), ext_set));

      std::vector<compute::Expression> expressions;
      for (const auto& expr : project.expressions()) {
        expressions.emplace_back();
        ARROW_ASSIGN_OR_RAISE(expressions.back(), FromProto(expr, ext_set));
      }

      return compute::Declaration::Sequence({
          std::move(input),
          {"project", compute::ProjectNodeOptions{std::move(expressions)}},
      });
    }

    case substrait::Rel::RelTypeCase::kAggregate: {
      const auto& aggregate = rel.aggregate();
      RETURN_NOT_OK(CheckRelCommon(aggregate));

      if (!aggregate.has_input()) {
        return Status::Invalid("substrait::AggregateRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input, FromProto(aggregate.input(), ext_set));

      compute::AggregateNodeOptions opts{{}, {}, {}};

      if (aggregate.groupings_size() > 1) {
        return Status::Invalid("substrait::AggregateRel has " +
                               std::to_string(aggregate.groupings_size()) + " groupings");
      }

      for (const auto& grouping : aggregate.groupings()) {
        for (const auto& expr : grouping.grouping_expressions()) {
          ARROW_ASSIGN_OR_RAISE(auto key_expr, FromProto(expr, ext_set));
          if (auto field = key_expr.field_ref()) {
            opts.keys.emplace_back(*field);
          } else {
            return Status::Invalid(
                "substrait::AggregateRel grouping key is not a field reference: " +
                key_expr.ToString());
          }
        }
      }

      // group by will first output targets then keys
      // We need a post projector to first output keys then targets
      // TODO: use options to control this behavior
      std::vector<compute::Expression> reordered_fields(opts.keys.size());
      int32_t reordered_field_pos = 0;

      for (const auto& measure : aggregate.measures()) {
        if (measure.has_filter()) {
          // invalid
          return Status::Invalid("substrait::AggregateRel has filter.");
        }

        auto agg_func = measure.measure();
        ARROW_ASSIGN_OR_RAISE(auto decoded_function,
                              ext_set.DecodeFunction(agg_func.function_reference()));

        if (!agg_func.sorts().empty()) {
          return Status::Invalid("substrait::AggregateRel aggregate function #" +
                                 decoded_function.name.to_string() + " has sort.");
        }

        std::vector<FieldRef> target_fields;
        target_fields.reserve(agg_func.args_size());
        for (const auto& arg : agg_func.args()) {
          ARROW_ASSIGN_OR_RAISE(auto target_expr, FromProto(arg, ext_set));
          if (auto target_field = target_expr.field_ref()) {
            target_fields.emplace_back(*target_field);
          } else {
            return Status::Invalid(
                "substrait::AggregateRel measure's arg is not a field reference: " +
                target_expr.ToString());
          }
        }

        int32_t target_field_idx = 0;
        if (decoded_function.name == "mean") {
          switch (agg_func.phase()) {
            case ::substrait::AggregationPhase::
                AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE: {
              for (const std::string& func : {"hash_sum", "hash_count"}) {
                opts.aggregates.push_back({func, nullptr});
                opts.targets.emplace_back(target_fields[target_field_idx]);
                opts.names.emplace_back(func + " " + opts.targets.back().ToString());
                reordered_fields.emplace_back(compute::field_ref(reordered_field_pos++));
              }
              target_field_idx++;
              break;
            }
            case ::substrait::AggregationPhase::
                AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT: {
              for (const std::string& func : {"hash_sum", "hash_sum"}) {
                opts.aggregates.push_back({func, nullptr});
                opts.targets.emplace_back(std::move(target_fields[target_field_idx]));
                opts.names.emplace_back(func + " " + opts.targets.back().ToString());
                target_field_idx++;
              }
              reordered_fields.emplace_back(
                  compute::call("divide", {compute::field_ref(reordered_field_pos++),
                                           compute::field_ref(reordered_field_pos++)}));
              break;
            }
            default:
              return Status::Invalid("substrait::AggregateRel unsupported phase " +
                                     std::to_string(agg_func.phase()));
          }
        } else if (decoded_function.name == "count" &&
                   agg_func.phase() == ::substrait::AggregationPhase::
                                           AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT) {
          std::string func = "hash_sum";
          opts.aggregates.push_back({func, nullptr});
          opts.targets.emplace_back(std::move(target_fields[target_field_idx]));
          opts.names.emplace_back(func + " " + opts.targets.back().ToString());
          target_field_idx++;
          reordered_fields.emplace_back(compute::field_ref(reordered_field_pos++));
        } else {
          std::string func = opts.keys.empty()
                                 ? decoded_function.name.to_string()
                                 : "hash_" + decoded_function.name.to_string();
          opts.aggregates.push_back({func, nullptr});
          opts.targets.emplace_back(std::move(target_fields[target_field_idx]));
          opts.names.emplace_back(func + " " + opts.targets.back().ToString());
          target_field_idx++;
          reordered_fields.emplace_back(compute::field_ref(reordered_field_pos++));
        }

        if (target_field_idx != agg_func.args_size()) {
          return Status::Invalid("substrait::AggregateRel aggregate function #" +
                                 decoded_function.name.to_string() +
                                 " not all arguments are consumed.");
        }
      }

      for (size_t i = 0; i < opts.keys.size(); ++i) {
        reordered_fields[i] = compute::field_ref(reordered_field_pos++);
      }

      auto aggregate_decl = compute::Declaration{"aggregate", std::move(opts)};

      auto post_project_decl = compute::Declaration{
          "project", compute::ProjectNodeOptions{std::move(reordered_fields)}};

      return compute::Declaration::Sequence(
          {std::move(input), std::move(aggregate_decl), std::move(post_project_decl)});
    }

    default:
      break;
  }

  return Status::NotImplemented(
      "conversion to arrow::compute::Declaration from Substrait relation ",
      rel.DebugString());
}

}  // namespace engine
}  // namespace arrow
