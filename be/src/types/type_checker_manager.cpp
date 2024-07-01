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


#include "types/type_checker_manager.h"
#include <fmt/format.h>

namespace starrocks {

TypeCheckerManager::TypeCheckerManager() {
    registerChecker("java.lang.Boolean", std::make_unique<BooleanTypeChecker>());
    // todo: 注册其他类型检查器
}

TypeCheckerManager& TypeCheckerManager::getInstance() {
    static TypeCheckerManager instance;
    return instance;
}

void TypeCheckerManager::registerChecker(const std::string& java_class, std::unique_ptr<TypeChecker> checker) {
    checkers[java_class] = std::move(checker);
}

StatusOr<LogicalType> TypeCheckerManager::checkType(const std::string& java_class, const SlotDescriptor* slot_desc) {
    auto it = checkers.find(java_class);
    if (it != checkers.end()) {
        return it->second->check(java_class, slot_desc);
    }
    return Status::NotSupported(
            fmt::format("JDBC result type of column[{}] is [{}], StarRocks does not recognize it, please set "
                        "the type of this column to varchar to avoid information loss.",
                        slot_desc->col_name(), java_class));
}

} // namespace starrocks