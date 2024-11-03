export const PLANNER_STATS_QUERY = `
  SELECT 
    attname as column_name,
    n_distinct::float8 as n_distinct,
    null_frac,
    avg_width,
    correlation,
    array_to_json(most_common_vals) as most_common_vals,
    most_common_freqs as most_common_freqs,
    array_to_json(histogram_bounds) as histogram_bounds
  FROM pg_stats
  WHERE schemaname = $1
  AND tablename = $2
  AND inherited = $3
  ORDER BY attname;
`;

export interface ColumnStats {
	column_name: string;
	n_distinct: number;
	null_frac: number;
	avg_width: number;
	correlation: number | null;
	most_common_vals: unknown[] | null; // Pre-parsed array
	most_common_freqs: number[] | null;
	histogram_bounds: unknown[] | null; // Pre-parsed array
	data_type: string;
}

export const TABLE_COLUMNS_QUERY = `
  SELECT 
    column_name, 
    data_type, 
    character_maximum_length,
    is_nullable,
    column_default,
    udt_name,
    is_identity,
    identity_generation,
    is_generated,
    generation_expression
  FROM information_schema.columns
  WHERE table_schema = $1 
  AND table_name = $2
  ORDER BY ordinal_position;
`;

export interface ColumnInfo {
	column_name: string;
	column_default?: string;
	data_type: string;
	is_nullable: "YES" | "NO";
	is_generated: "NEVER" | "ALWAYS";
	generation_expression: string | null;
}

export const PRIMARY_KEY_QUERY = `
  SELECT 
    a.attname as column_name,
    format_type(a.atttypid, a.atttypmod) as data_type,
    array_position(i.indkey, a.attnum) as key_order
  FROM pg_index i
  JOIN pg_class c ON c.oid = i.indrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
  WHERE i.indisprimary
  AND n.nspname = $1
  AND c.relname = $2
  ORDER BY key_order;
`;

export interface TablePrimaryKey {
	columnNames: string[];
	dataTypes: string[];
}

export const FOREIGN_KEY_QUERY = `
  SELECT
    ns2.nspname AS referenced_schema,
    cl2.relname AS referenced_table,
    json_agg(att1.attname ORDER BY att1.attnum) as referencing_columns,
    json_agg(att2.attname ORDER BY att2.attnum) as referenced_columns
  FROM pg_constraint con
  JOIN pg_class cl1 ON con.conrelid = cl1.oid
  JOIN pg_class cl2 ON con.confrelid = cl2.oid
  JOIN pg_namespace ns1 ON cl1.relnamespace = ns1.oid
  JOIN pg_namespace ns2 ON cl2.relnamespace = ns2.oid
  JOIN pg_attribute att1 ON att1.attrelid = con.conrelid AND att1.attnum = ANY(con.conkey)
  JOIN pg_attribute att2 ON att2.attrelid = con.confrelid AND att2.attnum = ANY(con.confkey)
  WHERE con.contype = 'f'
  AND ns1.nspname = $1
  AND cl1.relname = $2
  GROUP BY ns2.nspname, cl2.relname, con.conkey, con.confkey;
`;

export interface TableReference {
	referenced_schema: string;
	referenced_table: string;
	referencing_columns: string[];
	referenced_columns: string[];
}

export const ENUM_QUERY = `
  SELECT 
    t.typname as enum_name,
    json_agg(e.enumlabel ORDER BY e.enumsortorder) as enum_values
  FROM pg_type t 
  JOIN pg_enum e ON t.oid = e.enumtypid  
  JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
  WHERE n.nspname = $1
  GROUP BY t.typname;
`;
