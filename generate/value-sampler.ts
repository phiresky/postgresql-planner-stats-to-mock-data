import { Config } from "../config";
import { ColumnInfo } from "../read/queries";
import { ColumnStats } from "../read/queries";

// Types for clarity
type DataType = string;
type SamplingStrategy = "discrete" | "continuous" | "custom";

interface TypeHandler {
  strategy: SamplingStrategy;
  fallback:
    | "force-common-value"
    | ((columnStats: ColumnStats, config?: Config) => any);
  transform?: (value: any) => any;
}

// Common sampling functions
const samplingUtils = {
  sampleFromFrequencies(stats: ColumnStats): any {
    if (!stats.most_common_vals || !stats.most_common_freqs) return null;

    const rand = Math.random() * this.totalCommonFreq(stats);
    let cumulativeProb = 0;

    for (let i = 0; i < stats.most_common_vals.length; i++) {
      cumulativeProb += stats.most_common_freqs[i];
      if (rand < cumulativeProb) {
        return stats.most_common_vals[i];
      }
    }
    throw Error("Failed to sample from common values");
  },

  sampleFromHistogram(stats: ColumnStats, isTimestamp = false): any {
    if (!stats.histogram_bounds || stats.histogram_bounds.length < 2)
      return null;

    try {
      let min: number, max: number;

      if (isTimestamp) {
        // For timestamps, convert to milliseconds for proper interpolation
        min = new Date(stats.histogram_bounds[0] as Date).getTime();
        max = new Date(
          stats.histogram_bounds[stats.histogram_bounds.length - 1] as Date
        ).getTime();
      } else {
        min = Number(stats.histogram_bounds[0]);
        max = Number(stats.histogram_bounds[stats.histogram_bounds.length - 1]);
      }

      // Check for invalid bounds
      if (isNaN(min) || isNaN(max)) {
        return null;
      }

      const value = min + Math.random() * (max - min);
      return isTimestamp ? new Date(value) : value;
    } catch (e) {
      console.warn("Error sampling from histogram:", e);
      return null;
    }
  },

  totalCommonFreq(stats: ColumnStats): number {
    if (!stats.most_common_freqs) throw Error("No common frequencies found");
    return stats.most_common_freqs.reduce((a, b) => a + b, 0);
  },

  shouldUseCommonValue(stats: ColumnStats): boolean {
    if (!stats.most_common_freqs) return false;
    return Math.random() < this.totalCommonFreq(stats);
  },
};

// Type-specific handlers
const typeHandlers: Record<string, TypeHandler> = {
  // Numeric types
  integer: {
    strategy: "continuous",
    fallback: () => Math.floor(Math.random() * 1000000),
    transform: Math.floor,
  },
  bigint: {
    strategy: "continuous",
    fallback: () => Math.floor(Math.random() * 1000000),
    transform: Math.floor,
  },
  numeric: {
    strategy: "continuous",
    fallback: () => Math.random() * 1000000,
  },
  double: {
    strategy: "continuous",
    fallback: () => Math.random() * 1000000,
  },
  smallint: {
    strategy: "continuous",
    fallback: () => Math.random() * 1000000,
    transform: Math.floor,
  },

  // Time types
  timestamp: {
    strategy: "continuous",
    fallback: (stats, config) => {
      const start =
        config?.config.startDate ??
        (stats.histogram_bounds?.[0]
          ? new Date(stats.histogram_bounds?.[0] as Date)
          : new Date("2020-01-01"));
      const end = config?.config.endDate || new Date();
      return new Date(
        start.getTime() + Math.random() * (end.getTime() - start.getTime())
      );
    },
    transform: (value: any) => new Date(value),
  },

  // Text types
  text: {
    strategy: "discrete",
    fallback: () => `value_${Math.random().toString(36).substring(7)}`,
  },
  varchar: {
    strategy: "discrete",
    fallback: () => `value_${Math.random().toString(36).substring(7)}`,
  },

  // Boolean
  boolean: {
    strategy: "discrete",
    fallback: () => Math.random() < 0.5,
  },

  // Special types
  uuid: {
    strategy: "custom",
    fallback: () =>
      "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
        const r = (Math.random() * 16) | 0;
        const v = c === "x" ? r : (r & 0x3) | 0x8;
        return v.toString(16);
      }),
  },

  inet: {
    strategy: "custom",
    fallback: () =>
      Array(4)
        .fill(0)
        .map(() => Math.floor(Math.random() * 256))
        .join("."),
  },

  json: {
    strategy: "discrete",
    fallback: "force-common-value",
  },
  jsonb: {
    strategy: "discrete",
    fallback: "force-common-value",
  },

  "user-defined": {
    strategy: "discrete",
    fallback: (stats) => {
      if (!stats.most_common_vals?.length) {
        if (stats.null_frac > 0.1) return null;
        throw new Error(
          `No valid enum values found for column '${
            stats.column_name
          }', detail: ${JSON.stringify(stats)}`
        );
      }
      return stats.most_common_vals[0];
    },
  },
};

// Main generation function
export function generateValue(
  columnInfo: ColumnInfo,
  columnStats: ColumnStats,
  foreignKeyValues: any[] | null,
  config: Config
): any {
  // Handle foreign keys
  if (foreignKeyValues?.length) {
    return foreignKeyValues[
      Math.floor(Math.random() * foreignKeyValues.length)
    ];
  }

  // Handle null values
  if (Math.random() < columnStats.null_frac) {
    return null;
  }

  // Get type handler (normalized to base type)
  const baseType = columnStats.data_type.toLowerCase().replace(/\s.*$/, "");
  const handler = typeHandlers[baseType] || {
    strategy: "discrete",
    fallback: () => {
      if (columnInfo.column_default) {
        return columnInfo.column_default.replace(/::.*$/, "").replace(/'/g, "");
      }
      if (columnInfo.is_nullable === "YES") return null;
      throw new Error(`Unsupported data type: ${columnStats.data_type}`);
    },
  };

  // Generate value based on strategy
  let value: any = undefined;

  if (
    handler.fallback === "force-common-value" ||
    samplingUtils.shouldUseCommonValue(columnStats)
  ) {
    value = samplingUtils.sampleFromFrequencies(columnStats);
  }
  if (value === undefined && handler.strategy === "continuous") {
    value = samplingUtils.sampleFromHistogram(
      columnStats,
      baseType === "timestamp"
    );
  }

  if (value === undefined) {
    if (typeof handler.fallback !== "function")
      throw Error(
        `No fallback function for ${baseType}, ${JSON.stringify(columnStats)}`
      );
    value = handler.fallback(columnStats, config);
  }
  return handler.transform ? handler.transform(value) : value;
}
