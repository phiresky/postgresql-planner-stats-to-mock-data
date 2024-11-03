import { Pool } from "pg";

export interface UniqueIndex {
  schema: string;
  table: string;
  index_name: string;
  is_primary: boolean;
  columns: Array<{
    name: string;
    is_identity: boolean;
    has_default_nextval: boolean;
  }>;
}

export async function getTableUniqueIndexes(
  pool: Pool,
  schema: string,
  table: string
): Promise<UniqueIndex[]> {
  const query = `
    WITH index_info AS (
      SELECT 
        n.nspname as schema,
        c.relname as table,
        i.relname as index_name,
        idx.indisprimary as is_primary,
        json_agg(
          json_build_object(
            'name', a.attname,
            'is_identity', a.attidentity != '',
            'has_default_nextval', 
              CASE WHEN pg_get_expr(d.adbin, d.adrelid) LIKE 'nextval%' 
                   THEN true 
                   ELSE false 
              END
          )
          ORDER BY array_position(idx.indkey, a.attnum)
        ) as columns
      FROM pg_index idx
      JOIN pg_class i ON i.oid = idx.indexrelid
      JOIN pg_class c ON c.oid = idx.indrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_attribute a ON a.attrelid = idx.indrelid 
        AND a.attnum = ANY(idx.indkey)
      LEFT JOIN pg_attrdef d ON 
        d.adrelid = a.attrelid AND 
        d.adnum = a.attnum
      WHERE idx.indisunique = true  -- unique indexes
      AND n.nspname = $1
      AND c.relname = $2
      GROUP BY n.nspname, c.relname, i.relname, idx.indisprimary
    )
    SELECT 
      "schema",
      "table",
      index_name,
      is_primary,
      columns
    FROM index_info
    WHERE NOT (
      -- Filter out indexes where ALL columns are either identity or nextval
      (columns::jsonb @> '[{"is_identity": true}]'::jsonb OR
       columns::jsonb @> '[{"has_default_nextval": true}]'::jsonb)
      -- Keep primary key indexes even if they're identity/serial
      AND NOT is_primary
    );
  `;

  const result = await pool.query(query, [schema, table]);
  return result.rows;
}

export class UniqueIndexTracker {
  private usedValues: Set<string> = new Set();

  constructor(
    private index: UniqueIndex,
    private isPrimary: boolean = false
  ) {}

  isUnique(values: Record<string, any>): boolean {
    // Skip check if this is a primary key index and any of the columns
    // are identity/serial (they'll be auto-generated)
    if (this.isPrimary && this.index.columns.some(
      col => col.is_identity || col.has_default_nextval
    )) {
      return true;
    }

    // Create composite key from all index columns
    const key = this.index.columns
      .map(col => {
        const value = values[col.name];
        return value === null ? 'NULL' : String(value);
      })
      .join('|');

    if (this.usedValues.has(key)) {
      return false;
    }

    this.usedValues.add(key);
    return true;
  }

  getColumnNames(): string[] {
    return this.index.columns.map(col => col.name);
  }

  getIndexName(): string {
    return this.index.index_name;
  }
}