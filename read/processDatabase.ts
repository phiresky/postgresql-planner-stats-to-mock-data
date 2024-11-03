import { Pool } from "pg";
import * as fs from "fs/promises";
import * as path from "path";
import { getTableLoadOrder } from "./dependencies";
import { generateTableStatistics } from "./tableInfo";
import { TableDetails, DatabaseMetadata } from "./index";
import { isExcludedSchema } from "./constants";
import { Config } from "../config";

interface ProcessOptions {
  specificSchema?: string;
  specificTable?: string;
}

export async function processDatabase(
  config: Config,
  outputDir: string,
  options: ProcessOptions = {}
): Promise<void> {
  await ensureDir(outputDir);

  const { orderedTables, cycles, warnings } = await getTableLoadOrder(config);

  const tablesToProcess = orderedTables.filter((table) => {
    if (
      options.specificSchema &&
      table.schema_name !== options.specificSchema
    ) {
      return false;
    }
    if (
      options.specificSchema &&
      options.specificTable &&
      (table.schema_name !== options.specificSchema ||
        table.table_name !== options.specificTable)
    ) {
      return false;
    }
    return !isExcludedSchema(config, table.schema_name);
  });

  const tableDetails: TableDetails[] = [];

  for (const table of tablesToProcess) {
    const { schema_name, table_name } = table;

    const schemaDir = path.join(outputDir, schema_name);
    await ensureDir(schemaDir);

    try {
      process.stdout.write(`Processing ${schema_name}.${table_name} `);
      const startTime = process.hrtime();

      // Generate table statistics and details
      const {
        sql: statistics,
        columnInfo,
        plannerStats,
        sampleData,
        references,
        rowCount,
      } = await generateTableStatistics(config.pool, schema_name, table_name);

      // Write SQL to file
      const sqlFileName = `${table_name}.sql`;
      const sqlFilePath = path.join(schemaDir, sqlFileName);
      await fs.writeFile(sqlFilePath, statistics);

      // Add to table details with full statistics
      tableDetails.push({
        schema_name,
        table_name,
        sql_file: path.join(schema_name, sqlFileName),
        references,
        columnInfo,
        statistics: {
          rowCount,
          plannerStats,
          sampleData,
        },
      });

      const [seconds, nanoseconds] = process.hrtime(startTime);
      const milliseconds = Math.round(seconds * 1000 + nanoseconds / 1000000);
      process.stdout.write(`[${milliseconds} ms]\n`);
    } catch (error) {
      process.stdout.write("failed\n");
      console.error(`Error processing ${schema_name}.${table_name}:`, error);
      warnings.push(
        `Failed to process ${schema_name}.${table_name}: ${String(error)}`
      );
    }
  }

  // Write metadata JSON with full statistics
  const metadata: DatabaseMetadata = {
    tables: tableDetails,
    warnings,
    cycles,
  };

  await fs.writeFile(
    path.join(outputDir, "tables.json"),
    JSON.stringify(metadata, null, 2)
  );
}

async function ensureDir(dir: string): Promise<void> {
  await fs.mkdir(dir, { recursive: true });
}
