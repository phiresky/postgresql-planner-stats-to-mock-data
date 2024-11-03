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
  strategy: "skip";
};

export async function loadConfig(
  configPath?: string
): Promise<Config["config"]> {
  if (configPath) {
    return JSON.parse(await fs.readFile(configPath, "utf-8"));
  } else return { prodFraction: 0.1 };
}
