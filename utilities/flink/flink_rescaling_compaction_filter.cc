// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/flink/flink_rescaling_compaction_filter.h"

namespace ROCKSDB_NAMESPACE {
namespace flink {

int compare(const Slice& a, const Slice& b) {
  return a.compare(b);
}

FlinkRescalingCompactionFilter::ConfigHolder::ConfigHolder() 
    : config_(const_cast<FlinkRescalingCompactionFilter::Config*>(&DISABLED_RESCALING_CONFIG)){};

FlinkRescalingCompactionFilter::ConfigHolder::~ConfigHolder() {
  Config* config = config_.load();
  if (config != &DISABLED_RESCALING_CONFIG) {
    delete config;
  }
}

bool FlinkRescalingCompactionFilter::ConfigHolder::Configure(Config* config) {
  bool not_configured = GetConfig() == &DISABLED_RESCALING_CONFIG;
  if (not_configured) {
    config_ = config;
  }
  return not_configured;
}

FlinkRescalingCompactionFilter::Config*
FlinkRescalingCompactionFilter::ConfigHolder::GetConfig() {
  return config_.load();
}

FlinkRescalingCompactionFilter::FlinkRescalingCompactionFilter(
    std::shared_ptr<ConfigHolder> config_holder) 
    : FlinkRescalingCompactionFilter(std::move(config_holder), nullptr){};

FlinkRescalingCompactionFilter::FlinkRescalingCompactionFilter(
    std::shared_ptr<ConfigHolder> config_holder, std::shared_ptr<Logger> logger) 
    : config_holder_(std::move(config_holder)),
      logger_(std::move(logger)),
      config_cached_(const_cast<Config*>(&DISABLED_RESCALING_CONFIG)){};

inline void FlinkRescalingCompactionFilter::InitConfigIfNotYet() const {
  const_cast<FlinkRescalingCompactionFilter*>(this)->config_cached_ = 
      config_cached_ == &DISABLED_RESCALING_CONFIG ? config_holder_->GetConfig()
                                                   : config_cached_;
}

const char* FlinkRescalingCompactionFilter::Name() const {
    return "FlinkRescalingCompactionFilter";
}

CompactionFilter::Decision FlinkRescalingCompactionFilter::FilterV2(
    int /* level */, const Slice& key, ValueType /* value_type */,
    const Slice& existing_value, std::string* new_value,
    std::string* /*skip_until*/) const {

  InitConfigIfNotYet();

  const char key_rescale_round = existing_value.data()[0];

  Debug(logger_.get(),
        "Call FlinkRescalingCompactionFilter::FilterV2 - Key %s, rescale byte %d, "
        "Rescaling round: %d, smallest key: %s, largest key: %s",
        key.ToString().c_str(), config_cached_->rescale_round_, key_rescale_round,
        config_cached_->smallest_key_.ToString().c_str(), 
        config_cached_->largest_key_.ToString().c_str());

  const RescaleRound rescale_round = config_cached_->rescale_round_;
  
  if (key_rescale_round == rescale_round) {
    return Decision::kKeep;
  }

  // if key is in range [smallest, largest]
  if (compare(key, config_cached_->smallest_key_) >= 0 &&
      compare(key, config_cached_->largest_key_) <= 0) {
    new_value->clear();
    new_value->assign(existing_value.data(), existing_value.size());
    (*new_value)[0] = (char) rescale_round;
    Logger* logger = logger_.get();
    if (logger && logger->GetInfoLogLevel() <= InfoLogLevel::DEBUG_LEVEL) {
      Debug(logger, "New value: %s", new_value->c_str());
    }
    return Decision::kChangeValue;
  } else {
    return Decision::kRemove;
  }
}

} // namespace flink
} // namespace ROCKSDB_NAMESPACE