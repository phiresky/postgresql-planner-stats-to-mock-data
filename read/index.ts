import { Pool } from "pg";
import { processDatabase } from "./processDatabase";
import { ColumnStats } from "./queries";
import { ColumnInfo } from "./queries";
import { TableReference } from "./queries";
import { loadConfig } from "../config";

export interface DatabaseMetadata {
  tables: TableDetails[];
  warnings: string[];
  cycles: string[];
}

export interface TableDetails {
  schema_name: string;
  table_name: string;
  sql_file: string;
  references: TableReference[];
  columnInfo: ColumnInfo[];
  statistics: {
    rowCount: number;
    plannerStats: ColumnStats[];
    sampleData: Record<string, any>[];
  };
}

export interface TableStats {
  pkMin: string | null;
  pkMax: string | null;
  approximateCount: number;
}

async function main() {
  const args = process.argv.slice(2);
  const outputDir = "./schema_with_samples";

  try {
    if (args.length < 1 || args.length > 4) {
      console.error(
        `Usage: ${process.argv[1]} postgresql://[...] [path/to/config.json] [schema_name] [table_name]`
      );
      process.exit(1);
    }
    const [connectionString, configPath, schemaName, tableName] = args;
    const config = {
      pool: new Pool({ connectionString }),
      config: await loadConfig(configPath),
    };
    console.log("output directory", outputDir);
    if (!schemaName && !tableName) {
      console.log("Processing entire database..., outputDir:", outputDir);
      await processDatabase(config, outputDir);
    } else if (schemaName && !tableName) {
      console.log(`Processing schema: ${schemaName}`);
      await processDatabase(config, outputDir, {
        specificSchema: schemaName,
      });
    } else if (schemaName && tableName) {
      console.log(`Processing table: ${schemaName}.${tableName}`);
      await processDatabase(config, outputDir, {
        specificSchema: schemaName,
        specificTable: tableName,
      });
    }

    console.log("Processing completed successfully.");
  } catch (error) {
    console.error("Error:", error);
    process.exit(1);
  } finally {
    process.exit(0);
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error("Fatal error:", error);
    process.exit(1);
  });
}
