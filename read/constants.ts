import { Config, ExcludedColumn } from "../config";

export const EXCLUDED_SCHEMAS = ["pg_catalog", "information_schema"];

// Types where MIN/MAX operations don't make sense or aren't efficient
export const SKIP_MINMAX_TYPES = new Set(["uuid"]);

export function isExcludedSchema(config: Config, schema: string): boolean {
  if (EXCLUDED_SCHEMAS.includes(schema)) return true;
  if (schema.startsWith("_timescaledb")) return true;
  return config.config.excluded?.schemas?.includes(schema) ?? false;
}

export function isExcludedTable(
  config: Config,
  schema: string,
  table: string
): boolean {
  return (
    config.config.excluded?.tables?.includes(`${schema}.${table}`) ?? false
  );
}

export function isExcludedColumn(
  config: Config,
  schema: string,
  table: string,
  column: string
): ExcludedColumn | undefined {
  return config.config.excluded?.columns?.find(
    (c) => c.column === `${schema}.${table}.${column}`
  );
}
