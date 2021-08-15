// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <include/rocksdb/env.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <string>
#include <utility>

#include "rocksdb/compaction_filter.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
namespace flink {
static const std::size_t BITS_PER_BYTE = static_cast<std::size_t>(8);
static const std::size_t RESCALE_BIT_SIZE = static_cast<std::size_t>(1);
static const int64_t JAVA_MIN_LONG = static_cast<int64_t>(0x8000000000000000);
static const int64_t JAVA_MAX_LONG = static_cast<int64_t>(0x7fffffffffffffff);
static const std::size_t JAVA_MAX_SIZE = static_cast<std::size_t>(0x7fffffff);

class FlinkRescalingCompactionFilter : public CompactionFilter {
 public:
  enum RescaleRound {
    Disabled,
    Zero,
    One
  };

  struct Config {
    RescaleRound rescale_round_;
    Slice smallest_key_;
    Slice largest_key_;
  };

  class ConfigHolder {
   public:
    explicit ConfigHolder();
    ~ConfigHolder();
    bool Configure(Config* config);
    Config* GetConfig();
   private:
    std::atomic<Config*> config_;
  };

  explicit FlinkRescalingCompactionFilter(std::shared_ptr<ConfigHolder> config_holder);

  explicit FlinkRescalingCompactionFilter(std::shared_ptr<ConfigHolder> config_holder,
                                          std::shared_ptr<Logger> logger);

  const char* Name() const override;
  Decision FilterV2(int level, const Slice& key, ValueType value_type,
                    const Slice& existing_value, std::string* new_value,
                    std::string* skip_until) const override;
  bool IgnoreSnapshots() const override { return true; }
 private:
  inline void InitConfigIfNotYet() const;

  std::shared_ptr<ConfigHolder> config_holder_;
  std::shared_ptr<Logger> logger_;
  Config* config_cached_;
};

static const FlinkRescalingCompactionFilter::Config DISABLED_RESCALING_CONFIG = 
    FlinkRescalingCompactionFilter::Config{FlinkRescalingCompactionFilter::RescaleRound::Disabled};

} // namespace flink
} // namespace ROCKSDB_NAMESPACE
