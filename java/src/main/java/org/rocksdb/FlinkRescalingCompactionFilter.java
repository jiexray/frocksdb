//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Just a Java wrapper around FlinkRescalingCompactionFilter implemented in C++.
 *
 * Note: this compaction filter is a special implementation, designed for usage only in Apache Flink
 * project.
 */
public class FlinkRescalingCompactionFilter extends AbstractCompactionFilter<Slice> {
  public enum RescaleRound {
    Disabled,
    Zero,
    One
  }

  public FlinkRescalingCompactionFilter(ConfigHolder configHolder) {
    this(configHolder, null);
  }

  public FlinkRescalingCompactionFilter(ConfigHolder configHolder, Logger logger) {
    super(createNewFlinkRescalingCompactionFilter0(configHolder.nativeHandle_, logger.nativeHandle_));
  }

  private native static long createNewFlinkRescalingCompactionFilter0(
    long configHolderHandle, long loggerHandle);
  private native static long createNewFlinkRescalingCompactionConfigHolder();
  private native static void disposeFlinkRescalingCompactionFilterConfigHolder(long configHolderHandle);
  private native static boolean configureFlinkRescalingCompactionFilter(long configHolderHandle,
      int rescaleRound, byte[] smallestKey, int smallestKeyLen, 
      byte[] largestKey, int largestKeyLen);
  
  public static class Config {
    final RescaleRound rescaleRound;
    final byte[] smallestKey;
    final byte[] largestKey;

    private Config(RescaleRound rescaleRound, byte[] smallestKey, byte[] largestKey) {
      this.rescaleRound = rescaleRound;
      this.smallestKey = smallestKey;
      this.largestKey = largestKey;
    }

    @SuppressWarnings("WeakerAccess")
    public static Config createForZero(byte[] smallestKey, byte[] largestKey) {
      return new Config(RescaleRound.Zero, smallestKey, largestKey);
    }

    public static Config createForOne(byte[] smallestKey, byte[] largestKey) {
      return new Config(RescaleRound.One, smallestKey, largestKey);
    }
  }

  private static class ConfigHolder extends RocksObject {
    ConfigHolder() {
      super(createNewFlinkRescalingCompactionConfigHolder());
    }

    @Override
    protected void disposeInternal(long handle) {
      disposeFlinkRescalingCompactionFilterConfigHolder(handle);
    }
  }

  public static class FlinkRescalingCompactionFilterFactory
    extends AbstractCompactionFilterFactory<FlinkRescalingCompactionFilter> {
    private final ConfigHolder configHolder;
    private final Logger logger;

    @SuppressWarnings("unused")
    public FlinkRescalingCompactionFilterFactory() {
      this(null);
    }

    @SuppressWarnings("WeakerAccess")
    public FlinkRescalingCompactionFilterFactory(Logger logger) {
      this.configHolder = new ConfigHolder();
      this.logger = logger;
    }

    @Override
    public void close() {
      super.close();
      configHolder.close();
      if (logger != null) {
        logger.close();
      }
    }

    @Override
    public FlinkRescalingCompactionFilter createCompactionFilter(Context context) {
      return new FlinkRescalingCompactionFilter(configHolder, logger);
    }

    @Override
    public String name() {
      return "FlinkRescalingCompactionFilterFactory";
    }

    @SuppressWarnings("WeakerAccess")
    public void configure(Config config) {
      boolean already_configured = 
          !configureFlinkRescalingCompactionFilter(configHolder.nativeHandle_, config.rescaleRound.ordinal(),
              config.smallestKey, config.smallestKey.length,
              config.largestKey, config.largestKey.length);
      if (already_configured) {
        throw new IllegalStateException("Compaction filter is already configured");
      }
    }
  }

}