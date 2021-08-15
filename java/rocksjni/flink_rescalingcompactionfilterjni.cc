#include <climits>  // Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <include/rocksdb/env.h>
#include <jni.h>

#include "include/org_rocksdb_FlinkRescalingCompactionFilter.h"
#include "loggerjnicallback.h"
#include "portal.h"
#include "rocksjni/jnicallback.h"
#include "utilities/flink/flink_rescaling_compaction_filter.h"

using namespace ROCKSDB_NAMESPACE::flink;

/*
 * Class:     org_rocksdb_FlinkRescalingCompactionFilter
 * Method:    createNewFlinkRescalingCompactionConfigHolder
 * Signature: ()J
 */
jlong Java_org_rocksdb_FlinkRescalingCompactionFilter_createNewFlinkRescalingCompactionConfigHolder(
JNIEnv* /* env */, jclass /* jcls */) {
  using namespace ROCKSDB_NAMESPACE::flink;
  return reinterpret_cast<jlong>(new std::shared_ptr<FlinkRescalingCompactionFilter::ConfigHolder>(
    new FlinkRescalingCompactionFilter::ConfigHolder()));
}

/*
 * Class:     org_rocksdb_FlinkRescalingCompactionFilter
 * Method:    disposeFlinkRescalingCompactionFilterConfigHolder
 * Signature: (J)V
 */
void Java_org_rocksdb_FlinkRescalingCompactionFilter_disposeFlinkRescalingCompactionFilterConfigHolder(
    JNIEnv* /* env */, jclass /* jcls */, jlong handle) {
  using namespace ROCKSDB_NAMESPACE::flink;
  auto* config_holder =
      reinterpret_cast<std::shared_ptr<FlinkRescalingCompactionFilter::ConfigHolder>*>(
          handle);
  delete config_holder;
}

/*
 * Class:     org_rocksdb_FlinkRescalingCompactionFilter
 * Method:    createNewFlinkRescalingCompactionFilter0
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_FlinkRescalingCompactionFilter_createNewFlinkRescalingCompactionFilter0(
    JNIEnv* /* env */, jclass /* jcls */, jlong config_holder_handle,
    jlong logger_handle) {
  using namespace ROCKSDB_NAMESPACE::flink;
  auto config_holder = *(reinterpret_cast<std::shared_ptr<FlinkRescalingCompactionFilter::ConfigHolder>*>(
    config_holder_handle));
  auto logger = 
      logger_handle == 0 
          ? nullptr 
          : *(reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::LoggerJniCallback>*>(
            logger_handle));
  return reinterpret_cast<jlong>(new FlinkRescalingCompactionFilter(
    config_holder,
    logger));
}

/**
 * Class:      org_rocksdb_FlinkRescalingCompactionFilter
 * Method:     createNewFlinkRescalingCompactionFilter0
 * Signature:  ?
 */
jboolean Java_org_rocksdb_FlinkRescalingCompactionFilter_configureFlinkRescalingCompactionFilter(
    JNIEnv* env, jclass /* jcls */, jlong handle, jint ji_rescale_round,
    jbyteArray j_smallest_key, jint j_smallest_key_len, 
    jbyteArray j_largest_key, jint j_largest_key_len) {
  auto rescale_round = static_cast<FlinkRescalingCompactionFilter::RescaleRound>(ji_rescale_round);
  auto config_holder = 
      *(reinterpret_cast<std::shared_ptr<FlinkRescalingCompactionFilter::ConfigHolder>*>(
          handle));
  jbyte* smallest_key = new jbyte[j_smallest_key_len];
  env->GetByteArrayRegion(j_smallest_key, 0, j_smallest_key_len, smallest_key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] smallest_key;
    return false;
  }
  
  jbyte* largest_key = new jbyte[j_largest_key_len];
  env->GetByteArrayRegion(j_largest_key, 0, j_largest_key_len, largest_key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] largest_key;
    return false;
  }

  ROCKSDB_NAMESPACE::Slice smallest_key_slice(reinterpret_cast<char*>(smallest_key), j_smallest_key_len);
  ROCKSDB_NAMESPACE::Slice largest_key_slice(reinterpret_cast<char*>(largest_key), j_largest_key_len);

  auto config = new FlinkRescalingCompactionFilter::Config{rescale_round, smallest_key_slice, largest_key_slice};
  return static_cast<jboolean>(config_holder->Configure(config));
}