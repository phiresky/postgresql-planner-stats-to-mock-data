import { Pool } from "pg";
import { promises as fs } from "node:fs";
import { DatabaseMetadata, TableDetails } from "../read";
import { Config, loadConfig } from "../config";
import { ColumnInfo } from "../read/queries";
import {
  isExcludedColumn,
  isExcludedSchema,
  isExcludedTable,
} from "../read/constants";
import { generateValue } from "./value-sampler";

const foreignKeyCache = new Map<string, any[]>();

async function getActualRowCount(
  pool: Pool,
  schema: string,
  table: string
): Promise<number> {
  const query = `
    SELECT reltuples::bigint as approximate_count
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1
    AND c.relname = $2;
  `;

  const result = await pool.query(query, [schema, table]);
  const approximateCount = parseInt(result.rows[0]?.approximate_count || "0");

  // If approximate count is very low or zero, do an exact count
  if (approximateCount < 1000) {
    const exactQuery = `
      SELECT COUNT(*) as exact_count 
      FROM "${schema}"."${table}";
    `;
    const exactResult = await pool.query(exactQuery);
    return parseInt(exactResult.rows[0].exact_count);
  }

  return approximateCount;
}

async function getForeignKeyValues(
  pool: Pool,
  schema: string,
  table: string,
  columns: string[],
  sampleSize: number = 50
): Promise<any[]> {
  if (columns.length > 1)
    throw new Error("Composite foreign keys are not supported");
  const cacheKey = `${schema}.${table}.${columns.join(",")}`;

  if (foreignKeyCache.has(cacheKey)) {
    return foreignKeyCache.get(cacheKey)!;
  }

  // Get actual row count from the mock database
  const actualRowCount = await getActualRowCount(pool, schema, table);

  if (actualRowCount === 0) {
    throw new Error(
      `Referenced table ${schema}.${table} has no rows. Ensure tables are populated in the correct order.`
    );
  }

  // Use same sampling logic as in schema extractor
  const samplingMethod = actualRowCount < 1000000 ? "BERNOULLI" : "SYSTEM";
  const oversample = samplingMethod === "SYSTEM" ? 20000 : 2;
  const samplingPercentage = Math.min(
    100,
    (sampleSize / actualRowCount) * 100 * oversample
  );

  const columnList = columns.map((c) => `"${c}"`).join(", ");

  // For small tables, just query directly
  if (actualRowCount < 1000) {
    const directQuery = `
      SELECT ${columnList}
      FROM "${schema}"."${table}"
      WHERE ${columns.map((c) => `"${c}" IS NOT NULL`).join(" AND ")}
      ORDER BY random()
      LIMIT $1;
    `;

    const directResult = await pool.query(directQuery, [sampleSize]);
    const values = directResult.rows;
    foreignKeyCache.set(cacheKey, values);
    return values;
  }

  const query = `
    SELECT ${columnList}
    FROM "${schema}"."${table}"
    TABLESAMPLE ${samplingMethod} (${samplingPercentage})
    WHERE ${columns.map((c) => `"${c}" IS NOT NULL`).join(" AND ")}
    ORDER BY random()
    LIMIT $1;
  `;

  const result = await pool.query(query, [sampleSize]);
  let values = result.rows;

  // Fallback to direct query if sampling didn't get enough values
  if (values.length < sampleSize / 2 && actualRowCount < 1000000) {
    console.warn(
      `Sampling returned insufficient values for ${schema}.${table}, trying direct query`
    );
    const directQuery = `
      SELECT ${columnList}
      FROM "${schema}"."${table}"
      WHERE ${columns.map((c) => `"${c}" IS NOT NULL`).join(" AND ")}
      ORDER BY random()
      LIMIT $1;
    `;

    const directResult = await pool.query(directQuery, [sampleSize * 2]);
    if (directResult.rows.length > values.length) {
      values = directResult.rows;
    }
  }

  if (values.length === 0) {
    throw new Error(
      `No valid foreign key values found in ${schema}.${table} for columns: ${columns.join(
        ", "
      )}`
    );
  }

  foreignKeyCache.set(cacheKey, values);
  return values;
}

async function generateRowData(
  table: TableDetails,
  config: Config,
  prodFraction: number,
  foreignKeyData: Map<string, any[]>
): Promise<Record<string, any>[]> {
  const targetRowCount = Math.ceil(table.statistics.rowCount * prodFraction);
  const rows: Record<string, any>[] = [];

  for (let i = 0; i < targetRowCount; i++) {
    const row: Record<string, any> = {};

    for (const column of table.columnInfo) {
      if (column.is_generated !== "NEVER") continue;
      if (
        isExcludedColumn(
          config,
          table.schema_name,
          table.table_name,
          column.column_name
        )
      )
        continue;
      // Skip auto-incrementing columns
      if (
        column.column_default?.includes("nextval(") ||
        column.data_type.toLowerCase().includes("serial")
      ) {
        continue;
      }

      const stats = table.statistics.plannerStats.find(
        (s) => s.column_name === column.column_name
      );

      if (!stats) {
        console.warn(`No statistics found for column ${column.column_name}`);
        continue;
      }

      const fkReference = table.references.find((ref) =>
        ref.referencing_columns.includes(column.column_name)
      );

      const fkValues = fkReference
        ? foreignKeyData.get(
            `${fkReference.referenced_schema}.${fkReference.referenced_table}`
          ) ?? null
        : null;

      row[column.column_name] = generateValue(column, stats, fkValues, config);
    }

    rows.push(row);
  }

  return rows;
}

async function insertRows(
  pool: Pool,
  schema: string,
  table: string,
  columnInfo: ColumnInfo[],
  rows: Record<string, any>[]
): Promise<void> {
  if (rows.length === 0) return;
  const columnTypes = new Map(
    columnInfo.map((c) => [c.column_name, c.data_type])
  );

  const columns = Object.keys(rows[0]);
  const toPg = (dataType: string, value: unknown) =>
    ["json", "jsonb"].includes(dataType) ? JSON.stringify(value) : value;
  const values = rows.map((row) =>
    columns.map((col) => toPg(columnTypes.get(col)!, row[col]))
  );

  const query = `
    INSERT INTO "${schema}"."${table}" (${columns
    .map((c) => `"${c}"`)
    .join(", ")})
    VALUES ${values
      .map(
        (_, i) =>
          `(${columns
            .map((_, j) => `$${i * columns.length + j + 1}`)
            .join(", ")})`
      )
      .join(", ")}
  `;
  await pool.query(query, values.flat());
}

async function generateTableData(
  tableProgress: string,
  table: TableDetails,
  config: Config,
  metadata: DatabaseMetadata
): Promise<void> {
  console.log(`Generating data for ${table.schema_name}.${table.table_name}`);

  // Get foreign key values
  const foreignKeyData = new Map<string, any[]>();

  for (const ref of table.references) {
    try {
      const values = await getForeignKeyValues(
        config.pool,
        ref.referenced_schema,
        ref.referenced_table,
        ref.referenced_columns,
        1000
      );

      foreignKeyData.set(
        `${ref.referenced_schema}.${ref.referenced_table}`,
        values.map((m) => m[ref.referenced_columns[0]])
      );

      console.log(
        `Got ${values.length} foreign key values from ${ref.referenced_schema}.${ref.referenced_table}`
      );
    } catch (error) {
      throw new Error(
        `Failed to get foreign key values for ${table.schema_name}.${table.table_name} ` +
          `referencing ${ref.referenced_schema}.${ref.referenced_table}:`,
        { cause: error }
      );
    }
  }

  // Generate and insert rows in batches
  const batchSize = config.config.insertBatchSize ?? 2000;
  const targetRows = Math.ceil(
    table.statistics.rowCount * config.config.prodFraction
  );

  for (let i = 0; i < targetRows; i += batchSize) {
    const currentBatchSize = Math.min(batchSize, targetRows - i);
    const rows = await generateRowData(
      table,
      config,
      currentBatchSize / table.statistics.rowCount,
      foreignKeyData
    );
    await insertRows(
      config.pool,
      table.schema_name,
      table.table_name,
      table.columnInfo,
      rows
    );

    console.log(
      `Table ${tableProgress} ${table.schema_name}.${
        table.table_name
      }: Inserted ${Math.min(i + batchSize, targetRows)}/${targetRows} rows`
    );
  }
}

export async function populateDatabase(
  jsonPath: string,
  config: Config
): Promise<void> {
  const content = await fs.readFile(jsonPath, "utf-8");
  const metadata: DatabaseMetadata = JSON.parse(content);

  // Process tables in dependency order
  for (const [i, table] of metadata.tables.entries()) {
    if (isExcludedSchema(config, table.schema_name)) continue;
    if (isExcludedTable(config, table.schema_name, table.table_name)) continue;
    try {
      await generateTableData(
        `${i + 1}/${metadata.tables.length}`,
        table,
        config,
        metadata
      );
    } catch (error) {
      throw new Error(
        `Error generating data for ${table.schema_name}.${table.table_name}:`,
        { cause: error }
      );
    }
  }
  console.log("Data generation completed successfully.");
}

async function main() {
  if (process.argv.length !== 4 && process.argv.length !== 5) {
    console.error(
      `Usage: ${process.argv[1]} postgresql://[connectionString] path/to/tables.json [path/to/config.json]`
    );
    process.exit(1);
  }
  const [, , connectionString, tablesPath, configPath] = process.argv;
  const pool = new Pool({
    connectionString,
  });
  const config = await loadConfig(configPath);
  await populateDatabase(tablesPath, {
    pool,
    config,
  });
  process.exit(0);
}
if (require.main === module) {
  main().catch(console.error);
}
