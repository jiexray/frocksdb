// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/flink/flink_rescaling_compaction_filter.h"

#include <random>

#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {
namespace flink {

#define DISABLE FlinkRescalingCompactionFilter::RescaleRound::Disabled
#define ZERO FlinkRescalingCompactionFilter::RescaleRound::Zero
#define ONE FlinkRescalingCompactionFilter::RescaleRound::One

#define KVALUE CompactionFilter::ValueType::kValue
#define KMERGE CompactionFilter::ValueType::kMergeOperand
#define KBLOB CompactionFilter::ValueType::kBlobIndex

#define KKEEP CompactionFilter::Decision::kKeep
#define KREMOVE CompactionFilter::Decision::kRemove
#define KCHANGE CompactionFilter::Decision::kChangeValue

class ConsoleLogger : public Logger {
 public:
  using Logger::Logv;
  ConsoleLogger() : Logger(InfoLogLevel::DEBUG_LEVEL) {}

  void Logv(const char* format, va_list ap) override {
    vprintf(format, ap);
    printf("\n");
  }
};

Slice key;  // NOLINT
char data[24];
std::string new_list = "";  // NOLINT
std::string stub = "";      // NOLINT

FlinkRescalingCompactionFilter::RescaleRound rescale_round;
Slice largest_key;
Slice smallest_key;
CompactionFilter::ValueType value_type;
FlinkRescalingCompactionFilter* filter; // NOLINT

CompactionFilter::Decision decide() {
  return filter->FilterV2(0, key, value_type, Slice(data), &new_list, &stub);
}

void Init(
  FlinkRescalingCompactionFilter::RescaleRound rround,
  FlinkRescalingCompactionFilter::RescaleRound vround,
  const Slice& skey,
  const Slice& lkey,
  const Slice& vkey = Slice("key")) {
  rescale_round = rround;
  smallest_key = skey;
  largest_key = lkey;
  value_type = CompactionFilter::ValueType::kValue;
  data[0] = vround;
  key = vkey;
  
  auto config_holder = std::make_shared<FlinkRescalingCompactionFilter::ConfigHolder>();
  auto logger = std::make_shared<ConsoleLogger>();

  filter = new FlinkRescalingCompactionFilter(config_holder, logger);
  auto config = new FlinkRescalingCompactionFilter::Config{rescale_round, smallest_key, largest_key};
  EXPECT_TRUE(config_holder->Configure(config));
  EXPECT_FALSE(config_holder->Configure(config));
}

void Deinit() {delete filter;}

TEST(FlinkStateRescaleTest, CheckRescaleRoundEnumOrder) {
  EXPECT_EQ(DISABLE, 0);
  EXPECT_EQ(ZERO, 1);
  EXPECT_EQ(ONE, 2);
}

TEST(FlinkStateRescaleTest, CurrentRound) {
  Init(FlinkRescalingCompactionFilter::RescaleRound::One, 
       FlinkRescalingCompactionFilter::RescaleRound::One,
       Slice("000"), Slice("111"));
  EXPECT_EQ(decide(), KKEEP);
  Deinit();
}

TEST(FlinkStateRescaleTest, CurrentRound2) {
  Init(FlinkRescalingCompactionFilter::RescaleRound::Zero, 
       FlinkRescalingCompactionFilter::RescaleRound::Zero,
       Slice("000"), Slice("111"));
  EXPECT_EQ(decide(), KKEEP);
  Deinit();
}

TEST(FlinkStateRescaleTest, NotCurrentRoundInRange) {
  Init(FlinkRescalingCompactionFilter::RescaleRound::Zero, 
       FlinkRescalingCompactionFilter::RescaleRound::One,
       Slice("000"), Slice("111"), Slice("000"));
  EXPECT_EQ(decide(), KCHANGE);
  EXPECT_EQ(new_list.data()[0], FlinkRescalingCompactionFilter::RescaleRound::Zero);
  Deinit();

  Init(FlinkRescalingCompactionFilter::RescaleRound::Zero, 
       FlinkRescalingCompactionFilter::RescaleRound::One,
       Slice("000"), Slice("111"), Slice("111"));
  EXPECT_EQ(decide(), KCHANGE);
  EXPECT_EQ(new_list.data()[0], FlinkRescalingCompactionFilter::RescaleRound::Zero);
  Deinit();
}

TEST(FlinkStateRescaleTest, NotCurrentRoundNotInRange) {
  Init(FlinkRescalingCompactionFilter::RescaleRound::Zero, 
       FlinkRescalingCompactionFilter::RescaleRound::One,
       Slice("100"), Slice("111"), Slice("000"));
  EXPECT_EQ(decide(), KREMOVE);
  Deinit();

  Init(FlinkRescalingCompactionFilter::RescaleRound::Zero, 
       FlinkRescalingCompactionFilter::RescaleRound::One,
       Slice("000"), Slice("110"), Slice("111"));
  EXPECT_EQ(decide(), KREMOVE);
  Deinit();
}

}  // namespace flink
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
