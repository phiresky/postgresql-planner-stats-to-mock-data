import { Pool } from "pg";
import { TableReference } from "./queries";
import { ColumnInfo } from "./queries";
import {
	ColumnStats,
	FOREIGN_KEY_QUERY,
	PLANNER_STATS_QUERY,
	PRIMARY_KEY_QUERY,
	TABLE_COLUMNS_QUERY,
	TablePrimaryKey,
} from "./queries";
import { getCreateTableStatement } from "./tableStructure";

interface GenerateTableStatisticsResult {
	sql: string;
	columnInfo: ColumnInfo[];
	plannerStats: ColumnStats[];
	sampleData: Record<string, any>[];
	references: TableReference[];
	rowCount: number;
}

export async function generateTableStatistics(
	pool: Pool,
	schemaName: string,
	tableName: string,
): Promise<GenerateTableStatisticsResult> {
	const { count: rowCount, partitioned } = await getTableRowCount(
		pool,
		schemaName,
		tableName,
	);

	const columns = await getTableColumns(pool, schemaName, tableName);
	// Get all table information in parallel
	const [createStatement, pk, plannerStats, references] = await Promise.all([
		getCreateTableStatement(pool, schemaName, tableName),
		getTablePrimaryKey(pool, schemaName, tableName),
		getPlannerStats(pool, schemaName, tableName, partitioned, columns),
		getTableReferences(pool, schemaName, tableName),
	]);

	// Get samples and stats in parallel
	const [samples] = await Promise.all([
		getSampleRows(pool, schemaName, tableName, rowCount),
	]);

	// Format the SQL output
	const lines: string[] = [
		"-- Schema Definition",
		createStatement,
		"",
		"-- Table Information",
		`-- Table: ${schemaName}.${tableName}`,
	];

	if (pk) {
		const pkDisplay = pk.columnNames
			.map((col, i) => `${col} (${pk.dataTypes[i]})`)
			.join(", ");
		lines.push(`-- Primary Key: (${pkDisplay})`);
	} else {
		lines.push("-- Primary Key: None");
	}

	lines.push(
		`-- ${rowCount === 0 ? "Exact" : "Approx"} Row Count: ${rowCount.toLocaleString()}`,
	);

	lines.push(...formatPlannerStats(plannerStats));
	lines.push(
		"",
		"-- Sample Data",
		generateMultiRowInsertStatement(
			schemaName,
			tableName,
			columns,
			samples,
		),
		"",
	);

	return {
		sql: lines.join("\n"),
		columnInfo: columns,
		plannerStats: plannerStats.map((stat) => ({
			...stat,
			most_common_vals: stat.most_common_vals,
			histogram_bounds: stat.histogram_bounds,
		})),
		sampleData: samples,
		references,
		rowCount,
	};
}

async function getTableRowCount(
	pool: Pool,
	schemaName: string,
	tableName: string,
): Promise<{ count: number; partitioned: boolean }> {
	// First check if table has child tables
	const partitionCheck = `
    SELECT EXISTS (
      SELECT 1
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = $1
      AND c.relname = $2
      AND EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhparent = c.oid)
    ) as has_children;
  `;

	const {
		rows: [{ has_children }],
	} = await pool.query(partitionCheck, [schemaName, tableName]);

	if (has_children) {
		const partitionCountQuery = `
      SELECT sum(c.reltuples::bigint) as total_count
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_inherits i ON i.inhrelid = c.oid
      JOIN pg_class parent ON i.inhparent = parent.oid
      JOIN pg_namespace parent_schema ON parent.relnamespace = parent_schema.oid
      WHERE parent_schema.nspname = $1
      AND parent.relname = $2;
    `;

		const result = await pool.query(partitionCountQuery, [
			schemaName,
			tableName,
		]);
		const approxCount = parseInt(result.rows[0].total_count) || 0;

		if (approxCount === 0) {
			return {
				partitioned: true,
				count: await getExactRowCount(pool, schemaName, tableName),
			};
		}

		return { partitioned: true, count: approxCount };
	} else {
		const approximateQuery = `
      SELECT reltuples::bigint as approx_count
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = $1
      AND c.relname = $2;
    `;

		const result = await pool.query(approximateQuery, [
			schemaName,
			tableName,
		]);
		const approximateCount = parseInt(result.rows[0].approx_count);

		if (approximateCount === 0) {
			return {
				partitioned: false,
				count: await getExactRowCount(pool, schemaName, tableName),
			};
		}

		return { partitioned: false, count: approximateCount };
	}
}

export async function getExactRowCount(
	pool: Pool,
	schemaName: string,
	tableName: string,
): Promise<number> {
	const exactQuery = `SELECT COUNT(*) as exact_count FROM "${schemaName}"."${tableName}";`;
	const exactResult = await pool.query(exactQuery);
	return parseInt(exactResult.rows[0].exact_count);
}

export async function getSampleRows(
	pool: Pool,
	schemaName: string,
	tableName: string,
	approximateCount: number,
): Promise<Record<string, any>[]> {
	const samplingMethod = approximateCount < 1000000 ? "BERNOULLI" : "SYSTEM";
	const targetSampleSize = 50;
	const oversample = samplingMethod === "SYSTEM" ? 20000 : 2;
	const samplingPercentage = Math.min(
		100,
		(targetSampleSize / approximateCount) * 100 * oversample,
	);

	const samplesQuery = `
    SELECT *
    FROM "${schemaName}"."${tableName}"
    TABLESAMPLE ${samplingMethod} (${samplingPercentage})
    ORDER BY random()
    LIMIT ${targetSampleSize};
  `;

	const samplesResult = await pool.query(samplesQuery);
	return samplesResult.rows;
}

async function getTablePrimaryKey(
	pool: Pool,
	schemaName: string,
	tableName: string,
): Promise<TablePrimaryKey | null> {
	const result = await pool.query(PRIMARY_KEY_QUERY, [schemaName, tableName]);

	if (result.rows.length === 0) {
		return null;
	}

	return {
		columnNames: result.rows.map((row) => row.column_name),
		dataTypes: result.rows.map((row) => row.data_type),
	};
}

async function getTableReferences(
	pool: Pool,
	schemaName: string,
	tableName: string,
): Promise<TableReference[]> {
	const result = await pool.query(FOREIGN_KEY_QUERY, [schemaName, tableName]);
	return result.rows;
}

async function getTableColumns(
	pool: Pool,
	schemaName: string,
	tableName: string,
): Promise<ColumnInfo[]> {
	const result = await pool.query<ColumnInfo>(TABLE_COLUMNS_QUERY, [
		schemaName,
		tableName,
	]);
	return result.rows;
}

function formatPlannerStats(stats: ColumnStats[]): string[] {
	const lines: string[] = [
		"",
		"-- Planner Statistics",
		"-- Collected by ANALYZE, used by the query planner",
	];

	const FREQ_THRESHOLD = 0.005; // 5%
	const MAX_COMMON_VALUES = 20;

	for (const stat of stats) {
		lines.push(`--`);
		lines.push(`-- Column: ${stat.column_name}`);
		lines.push(`--   Average Width: ${stat.avg_width} bytes`);
		lines.push(`--   Null Fraction: ${(stat.null_frac * 100).toFixed(2)}%`);

		if (stat.n_distinct === -1) {
			lines.push(`--   Distinct Values: all unique`);
		} else if (stat.n_distinct < 0) {
			lines.push(
				`--   Distinct Values: ${(-stat.n_distinct * 100).toFixed(2)}% of rows are unique`,
			);
		} else {
			lines.push(`--   Distinct Values: ${stat.n_distinct}`);
		}

		if (stat.correlation !== null && stat.correlation !== 0) {
			const strength =
				Math.abs(stat.correlation) > 0.7 ? "strong" : "moderate";
			const direction = stat.correlation > 0 ? "ascending" : "descending";
			lines.push(
				`--   Correlation: ${stat.correlation.toFixed(4)} ` +
					`(${strength} ${direction} correlation with physical storage order)`,
			);
		}

		const commonVals = stat.most_common_vals;
		if (
			commonVals &&
			commonVals.length > 0 &&
			stat.most_common_freqs &&
			stat.most_common_freqs[0] >= FREQ_THRESHOLD
		) {
			lines.push(`--   Most Common Values:`);
			commonVals.slice(0, MAX_COMMON_VALUES).forEach((val, i) => {
				const freq = stat.most_common_freqs![i];
				if (freq >= FREQ_THRESHOLD) {
					lines.push(
						`--     ${val}: ${(freq * 100).toFixed(2)}% of rows`,
					);
				}
			});
		}

		const bounds = stat.histogram_bounds;

		if (stat.correlation !== null && bounds && bounds.length >= 2) {
			lines.push(
				`--   Value Range: ${bounds[0]} to ${bounds[bounds.length - 1]}`,
			);
			lines.push(`--   Histogram Buckets: ${bounds.length - 1}`);
		}
	}

	return lines;
}

function generateMultiRowInsertStatement(
	schemaName: string,
	tableName: string,
	columns: ColumnInfo[],
	rows: Record<string, any>[],
): string {
	if (rows.length === 0) return "";

	const columnNames = columns.map((col) => `"${col.column_name}"`).join(", ");
	const valueRows = rows.map(
		(row) =>
			`  (${columns
				.map((col) => formatValue(row[col.column_name], col.data_type))
				.join(", ")})`,
	);

	return `INSERT INTO "${schemaName}"."${tableName}" (${columnNames}) VALUES\n${valueRows.join(",\n")};`;
}

function formatValue(value: any, dataType: string): string {
	if (value === null) return "NULL";

	switch (dataType) {
		case "character varying":
		case "text":
		case "char":
		case "character":
			return `'${value.replace(/'/g, "''")}'`;

		case "timestamp without time zone":
		case "timestamp with time zone":
		case "date":
			return `'${new Date(value).toJSON()}'`;

		case "boolean":
			return value.toString();

		case "json":
		case "jsonb":
			return `'${JSON.stringify(value).replace(/'/g, "''")}'`;

		case "ARRAY":
			if (Array.isArray(value)) {
				const formattedArray = value
					.map((v) => formatValue(v, dataType.replace("ARRAY", "")))
					.join(",");
				return `ARRAY[${formattedArray}]`;
			}
			return `ARRAY[]`;

		default:
			return value.toString();
	}
}

async function getPlannerStats(
	pool: Pool,
	schemaName: string,
	tableName: string,
	isPartitionedTable: boolean,
	columnInfo: ColumnInfo[],
): Promise<ColumnStats[]> {
	const result = await pool.query<ColumnStats>(PLANNER_STATS_QUERY, [
		schemaName,
		tableName,
		isPartitionedTable,
	]);

	// Parse array strings into proper arrays
	return result.rows.map((row) => ({
		...row,
		// most_common_vals: parsePostgresArray(row.most_common_vals),
		// histogram_bounds: parsePostgresArray(row.histogram_bounds),
		data_type: columnInfo.find(
			(col) => col.column_name === row.column_name,
		)!.data_type,
	}));
}
