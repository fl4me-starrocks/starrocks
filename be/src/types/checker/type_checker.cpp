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

#include "types/checker/type_checker.h"
#include <fmt/format.h>

namespace starrocks {
// java.lang.Byte
class ByteTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_BOOLEAN && type != TYPE_TINYINT && type != TYPE_SMALLINT && type != TYPE_INT &&
            type != TYPE_BIGINT) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is Byte, please set the type to "
                               "one of boolean,tinyint,smallint,int,bigint",
                               slot_desc->col_name());
                    )
        }
        if (type == TYPE_BOOLEAN) {
            return TYPE_BOOLEAN;
        }
        return TYPE_TINYINT;
    }
};

// com.clickhouse.data.value.UnsignedByte
class ClickHouseUnsignedByteTypeChecker : public TypeChecker {
public:
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_SMALLINT && type != TYPE_INT && type != TYPE_BIGINT) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is UnsignedByte, please set the type to "
                    "one of smallint,int,bigint",
                    slot_desc->col_name()));
        }
        return TYPE_SMALLINT;
    }
};

// java.lang.Short
class ShortTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_TINYINT && type != TYPE_SMALLINT && type != TYPE_INT && type != TYPE_BIGINT) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is Short, please set the type to "
                                "one of tinyint,smallint,int,bigint",
                                slot_desc->col_name()));
        }
        return TYPE_SMALLINT;
    }
};

// com.clickhouse.data.value.UnsignedShort
class ClickHouseUnsignedShortTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_INT && type != TYPE_BIGINT) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is UnsignedShort, please set the type to "
                    "one of int,bigint",
                    slot_desc->col_name()));
        }
        return TYPE_INT;
    }
};

// java.lang.Integer
class IntegerTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_TINYINT && type != TYPE_SMALLINT && type != TYPE_INT && type != TYPE_BIGINT) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is Integer, please set the type to "
                                "one of tinyint,smallint,int,bigint",
                                slot_desc->col_name()));
        }
        return TYPE_INT;
    }
};
// java.lang.String
class StringTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_CHAR && type != TYPE_VARCHAR) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is String, please set the type to varchar or char",
                    slot_desc->col_name()));
        }
        return TYPE_VARCHAR;
    }
};

// com.clickhouse.data.value.UnsignedInteger
class ClickHouseUnsignedIntegerTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_BIGINT) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is UnsignedInteger, please set the type to bigint",
                    slot_desc->col_name()));
        }
        return TYPE_BIGINT;
    }
};

// java.lang.Long
class LongTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_BIGINT) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is Long, please set the type to bigint",
                    slot_desc->col_name()));
        }
        return TYPE_BIGINT;
    }
};

// java.math.BigInteger
class BigIntegerTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_LARGEINT && type != TYPE_VARCHAR) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is BigInteger, please set the type to largeint",
                    slot_desc->col_name()));
        }
        return TYPE_VARCHAR;
    }
};

// com.clickhouse.data.value.UnsignedLong
class ClickHouseUnsignedLongTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_LARGEINT) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is UnsignedLong, please set the type to largeint",
                    slot_desc->col_name()));
        }
        return TYPE_VARCHAR;
    }
};

// java.lang.Boolean
class BooleanTypeChecker : public TypeChecker {
public:
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_BOOLEAN && type != TYPE_SMALLINT && type != TYPE_INT && type != TYPE_BIGINT) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is Boolean, please set the type to "
                                "one of boolean,smallint,int,bigint",
                                slot_desc->col_name());
            )
        }
        return TYPE_BOOLEAN;
    }
};

// java.lang.Float
class FloatTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_FLOAT) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is Float, please set the type to float",
                    slot_desc->col_name()));
        }
        return TYPE_FLOAT;
    }
};

// java.lang.Double
class DoubleTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_DOUBLE && type != TYPE_FLOAT) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is Double, please set the type to double/float",
                    slot_desc->col_name()));
        }
        return TYPE_DOUBLE;
    }
};

//  java.sql.Timestamp
class TimestampTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_DATETIME && type != TYPE_VARCHAR) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is Timestamp, please set the type to datetime",
                    slot_desc->col_name()));
        }
        return TYPE_VARCHAR;
    }
};

// java.sql.Date
class DateTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_DATE) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is Date, please set the type to date",
                                slot_desc->col_name()));
        }
        return TYPE_VARCHAR;
    }
};

// java.sql.Time
class TimeTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_TIME) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is Time, please set the type to time",
                                slot_desc->col_name()));
        }
        return TYPE_TIME;
    }
};

// java.time.LocalDateTime
class LocalDateTimeTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_DATETIME) {
            return Status::NotSupported(fmt::format(
                    "Type mismatches on column[{}], JDBC result type is LocalDateTime, please set the type to datetime",
                    slot_desc->col_name()));
        }
        return TYPE_VARCHAR;
    }
};

// java.math.BigDecimal
class BigDecimalTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_DECIMAL32 && type != TYPE_DECIMAL64 && type != TYPE_DECIMAL128 && type != TYPE_VARCHAR &&
            type != TYPE_DOUBLE) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is BigDecimal, please set the type to "
                                "decimal、double or varchar",
                                slot_desc->col_name()));
        }
        return TYPE_VARCHAR;
    }
};

// oracle.sql.TIMESTAMP  oracle.sql.TIMESTAMPLTZ oracle.sql.TIMESTAMPTZ
class OracleTimestampClassTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_VARCHAR) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is {}, please set the "
                                "type to varchar",
                                slot_desc->col_name(), java_class));
        }
        return TYPE_VARCHAR;
    }
};

// microsoft.sql.DateTimeOffset
class SqlServerDateTimeOffsetTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_VARCHAR) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is {}, please set the "
                                "type to varchar",
                                slot_desc->col_name(), java_class));
        }
        return TYPE_VARCHAR;
    }
};

// byte[] oracle.jdbc.OracleBlob [B
class ByteArrayTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_BINARY && type != TYPE_VARBINARY) {
            return Status::NotSupported(
                    fmt::format("Type mismatches on column[{}], JDBC result type is {}, please set the "
                                "type to varbinary",
                                slot_desc->col_name(), java_class));
        }
        return TYPE_VARBINARY;
    }
};


class DefaultTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override {
        auto type = slot_desc->type().type;
        if (type != TYPE_VARCHAR) {
            return Status::NotSupported(
                    fmt::format("JDBC result type of column[{}] is [{}], StarRocks does not recognize it, please set "
                                "the type of this column to varchar to avoid information loss.",
                                slot_desc->col_name(), java_class));
        }
        return TYPE_VARCHAR;
    }
};







}