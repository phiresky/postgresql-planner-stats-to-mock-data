import { Pool } from "pg";
import { promises as fs } from "fs";

// Add types used by both extract and generate phases

export interface Config {
  /**
   * Use this pg pool to get values for foreign key relations and to insert
   * the rows
   */
  pool: Pool;
  config: {
    /** rows are inserted in batches of this size. Use a value smaller than 10k */
    insertBatchSize?: number;
    /** A value between 0 and 1 that specifies how many rows to generate */
    prodFraction: number;
    /** If given, for any time based columns, use these start and end dates */
    startDate?: Date;
    endDate?: Date;

    excluded?: {
      schemas?: string[];
      tables?: string[];
      columns?: ExcludedColumn[];
    };
  };
}

export type ExcludedColumn = {
  column: string;
} & ({
  /** Skip this column entirely when generating mock data */
  strategy: "skip";
} | {
  /** Override the column's statistics with custom values instead of sampling from real data */
  strategy: "override";
  /** The statistics to use for this column */
  stats: {
    /** Fixed values to use instead of sampling. For example ["<hidden>"] for sensitive data */
    most_common_vals: unknown[];
    /** Frequency of each value in most_common_vals. Must sum to 1 or less.
     * Each value represents the probability of selecting the corresponding value from most_common_vals.
     * For example, [1.0] means always use the first value, [0.7, 0.3] means 70% chance of first value, 30% chance of second.
     */
    most_common_freqs: number[];
    /** Optional fraction of NULL values (0-1). Defaults to 0 if not specified. */
    null_frac?: number;
  };
});

export async function loadConfig(
  configPath?: string
): Promise<Config["config"]> {
  if (configPath) {
    return JSON.parse(await fs.readFile(configPath, "utf-8"));
  } else return { prodFraction: 0.1 };
}
