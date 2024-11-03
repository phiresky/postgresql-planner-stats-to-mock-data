import { Pool } from "pg";
import { isExcludedSchema } from "./constants";
import { Config } from "../config";

interface TableInfo {
  schema_name: string;
  table_name: string;
}

interface TableDependency {
  schema_name: string;
  table_name: string;
  referenced_schema: string;
  referenced_table: string;
}

interface TableNode {
  schema_name: string;
  table_name: string;
  dependencies: Set<string>; // Set of "schema.table" strings
  dependents: Set<string>; // Set of "schema.table" strings
}

type TableGraph = Map<string, TableNode>;

interface TableLoadOrderResult {
  orderedTables: TableInfo[];
  cycles: string[];
  warnings: string[];
}

export async function getTableLoadOrder(
  config: Config
): Promise<TableLoadOrderResult> {
  const warnings: string[] = [];

  // Get all tables and their dependencies
  const [tables, dependencies] = await Promise.all([
    getAllTables(config),
    getForeignKeyDependencies(config),
  ]);

  // Build dependency graph
  const graph = buildDependencyGraph(tables, dependencies);

  // Detect cycles
  const cycles = detectCycles(graph);
  if (cycles.length > 0) {
    warnings.push(
      "Circular dependencies detected. The ordering may not be perfect."
    );
    cycles.forEach((cycle) => {
      warnings.push(`Cycle found: ${cycle}`);
    });
  }

  // Order tables
  const orderedTables = orderTablesByDependency(graph);

  return {
    orderedTables,
    cycles,
    warnings,
  };
}

async function getAllTables(config: Config): Promise<TableInfo[]> {
  const query = `
    SELECT 
      table_schema as schema_name,
      table_name
    FROM information_schema.tables 
    WHERE table_type = 'BASE TABLE'
    ORDER BY table_schema, table_name;
  `;

  const result = await config.pool.query<TableInfo>(query);
  return result.rows.filter(
    (table) => !isExcludedSchema(config, table.schema_name)
  );
}

async function getForeignKeyDependencies(
  config: Config
): Promise<TableDependency[]> {
  const query = `
    SELECT
      cl1.relname AS table_name,
      ns1.nspname AS schema_name,
      cl2.relname AS referenced_table,
      ns2.nspname AS referenced_schema
    FROM pg_constraint con
    JOIN pg_class cl1 ON con.conrelid = cl1.oid
    JOIN pg_class cl2 ON con.confrelid = cl2.oid
    JOIN pg_namespace ns1 ON cl1.relnamespace = ns1.oid
    JOIN pg_namespace ns2 ON cl2.relnamespace = ns2.oid
    WHERE con.contype = 'f';
  `;

  const result = await config.pool.query<TableDependency>(query);
  return result.rows.filter(
    (dep) =>
      !isExcludedSchema(config, dep.schema_name) &&
      !isExcludedSchema(config, dep.referenced_schema)
  );
}

function buildDependencyGraph(
  tables: TableInfo[],
  dependencies: TableDependency[]
): TableGraph {
  const graph: TableGraph = new Map();

  // Initialize graph with all tables
  for (const table of tables) {
    const key = `${table.schema_name}.${table.table_name}`;
    graph.set(key, {
      schema_name: table.schema_name,
      table_name: table.table_name,
      dependencies: new Set(),
      dependents: new Set(),
    });
  }

  // Add dependencies
  for (const dep of dependencies) {
    const tableKey = `${dep.schema_name}.${dep.table_name}`;
    const refKey = `${dep.referenced_schema}.${dep.referenced_table}`;

    const tableNode = graph.get(tableKey);
    const refNode = graph.get(refKey);

    if (tableNode && refNode) {
      tableNode.dependencies.add(refKey);
      refNode.dependents.add(tableKey);
    }
  }

  return graph;
}

function detectCycles(graph: TableGraph): string[] {
  const visited = new Set<string>();
  const recursionStack = new Set<string>();
  const cycles: string[] = [];

  function dfs(nodeKey: string, path: string[] = []): boolean {
    if (recursionStack.has(nodeKey)) {
      const cycleStart = path.indexOf(nodeKey);
      cycles.push([...path.slice(cycleStart), nodeKey].join(" -> "));
      return true;
    }

    if (visited.has(nodeKey)) return false;

    visited.add(nodeKey);
    recursionStack.add(nodeKey);
    path.push(nodeKey);

    const node = graph.get(nodeKey);
    if (node) {
      for (const dep of node.dependencies) {
        if (dfs(dep, path)) return true;
      }
    }

    path.pop();
    recursionStack.delete(nodeKey);
    return false;
  }

  for (const nodeKey of graph.keys()) {
    if (!visited.has(nodeKey)) {
      dfs(nodeKey);
    }
  }

  return cycles;
}

function orderTablesByDependency(graph: TableGraph): TableInfo[] {
  const ordered: TableInfo[] = [];
  const visited = new Set<string>();

  function findIndependentNodes(): string[] {
    return Array.from(graph.entries())
      .filter(
        ([key, node]) =>
          !visited.has(key) &&
          Array.from(node.dependencies).every((dep) => visited.has(dep))
      )
      .map(([key]) => key);
  }

  while (visited.size < graph.size) {
    const independentNodes = findIndependentNodes();

    if (independentNodes.length === 0 && visited.size < graph.size) {
      // Break cycles by choosing the node with fewest dependencies
      const unvisitedNodes = Array.from(graph.entries())
        .filter(([key]) => !visited.has(key))
        .sort(
          ([, nodeA], [, nodeB]) =>
            nodeA.dependencies.size - nodeB.dependencies.size
        );

      if (unvisitedNodes.length > 0) {
        independentNodes.push(unvisitedNodes[0][0]);
      }
    }

    for (const nodeKey of independentNodes) {
      const node = graph.get(nodeKey);
      if (node) {
        ordered.push({
          schema_name: node.schema_name,
          table_name: node.table_name,
        });
        visited.add(nodeKey);
      }
    }
  }

  return ordered;
}
