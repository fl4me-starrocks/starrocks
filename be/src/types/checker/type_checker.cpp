// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#include "types/checker/type_checker.h"

namespace starrocks {
class BooleanTypeChecker : public TypeChecker {
public:
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        if (type != TYPE_BOOLOEAN && type != TYPE_SMALLINT && type != TYPE_INT && type != TYPE_BIGINT) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is Boolean, please set the type to "
                                "one of boolean,smallint,int,bigint",
                                slot_desc->col_name());
                    )
        }
        return TYPE_BOOLEAN;
    }
};
}