import { Pool } from "pg";
import { ENUM_QUERY } from "./queries";

export async function getCreateTableStatement(
	pool: Pool,
	schemaName: string,
	tableName: string,
): Promise<string> {
	// Get enum definitions first
	const enumDefinitions = await getEnumDefinitions(pool, schemaName);

	// Get column definitions and used enum types
	const { columnDefinitions, enumTypes } = await getColumnDefinitions(
		pool,
		schemaName,
		tableName,
	);

	// Get constraints
	const constraintsQuery = `
    SELECT pg_get_constraintdef(con.oid) as constraint_def
    FROM pg_constraint con
    INNER JOIN pg_class rel ON rel.oid = con.conrelid
    INNER JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
    WHERE nsp.nspname = $1
    AND rel.relname = $2;
  `;

	const constraintsResult = await pool.query(constraintsQuery, [
		schemaName,
		tableName,
	]);

	// Build the complete CREATE statement
	const lines: string[] = [];

	// Add relevant enum type definitions
	for (const enumType of enumTypes) {
		const enumDef = enumDefinitions.get(enumType);
		if (enumDef) {
			lines.push(enumDef, "");
		}
	}

	// Add CREATE TABLE statement
	lines.push(`CREATE TABLE "${schemaName}"."${tableName}" (`);

	// Add columns
	lines.push(columnDefinitions.map((col) => "  " + col).join(",\n"));

	// Add constraints if they exist
	if (constraintsResult.rows.length > 0) {
		lines.push(",");
		lines.push(
			constraintsResult.rows
				.map((row) => "  " + row.constraint_def)
				.join(",\n"),
		);
	}

	lines.push(");");

	return lines.join("\n");
}

async function getEnumDefinitions(
	pool: Pool,
	schemaName: string,
): Promise<Map<string, string>> {
	const result = await pool.query(ENUM_QUERY, [schemaName]);
	const enumMap = new Map<string, string>();

	for (const row of result.rows) {
		const createTypeStmt = `CREATE TYPE "${schemaName}"."${row.enum_name}" AS ENUM (\n  '${row.enum_values.join("',\n  '")}'\n);`;
		enumMap.set(row.enum_name, createTypeStmt);
	}

	return enumMap;
}

async function getColumnDefinitions(
	pool: Pool,
	schemaName: string,
	tableName: string,
): Promise<{
	columnDefinitions: string[];
	enumTypes: Set<string>;
}> {
	const columnsQuery = `
    SELECT 
      column_name,
      data_type,
      character_maximum_length,
      is_nullable,
      column_default,
      udt_name,
      is_identity,
      identity_generation
    FROM information_schema.columns
    WHERE table_schema = $1
    AND table_name = $2
    ORDER BY ordinal_position;
  `;

	const result = await pool.query(columnsQuery, [schemaName, tableName]);
	const enumTypes = new Set<string>();

	const columnDefinitions = result.rows.map((col) => {
		const parts = [];
		parts.push(`"${col.column_name}"`);

		// Handle enum types (USER-DEFINED) and other types
		if (col.data_type === "USER-DEFINED") {
			parts.push(col.udt_name); // This will be the enum type name
			enumTypes.add(col.udt_name);
		} else {
			let dataType = col.data_type;
			if (col.character_maximum_length) {
				dataType += `(${col.character_maximum_length})`;
			}
			parts.push(dataType);
		}

		if (col.is_nullable === "NO") {
			parts.push("NOT NULL");
		}

		if (col.column_default) {
			parts.push(`DEFAULT ${col.column_default}`);
		}

		if (col.is_identity === "YES") {
			const generation =
				col.identity_generation === "BY DEFAULT"
					? "BY DEFAULT"
					: "ALWAYS";
			parts.push(`GENERATED ${generation} AS IDENTITY`);
		}

		return parts.join(" ");
	});

	return { columnDefinitions, enumTypes };
}
