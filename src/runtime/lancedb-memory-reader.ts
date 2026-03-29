/**
 * Read-only LanceDB memory reader for memory-lancedb-pro.
 * Provides typed access to the "memories" table without any write operations.
 */

import * as path from "node:path";
import * as fs from "node:fs";
import { connect } from "@lancedb/lancedb";

export interface MemoryEntryRow {
  id: string;
  text: string;
  category: string;
  scope: string;
  importance: number;
  timestamp: number;
  metadata?: string;
}

export interface MemoryListOptions {
  category?: string;
  scope?: string;
  search?: string;
  limit?: number;
  offset?: number;
}

export interface MemoryListResult {
  entries: MemoryEntryRow[];
  total: number;
  categories: string[];
  scopes: string[];
}

const DEFAULT_DB_PATH = path.join(
  process.env.HOME || "/root",
  ".openclaw",
  "memory",
  "lancedb-pro",
);

const TABLE_NAME = "memories";

let cachedDb: ReturnType<typeof connect> extends Promise<infer T> ? T : never;
let cachedDbPath: string | null = null;

async function getDb(dbPath?: string) {
  const resolvedPath = dbPath || DEFAULT_DB_PATH;
  if (!fs.existsSync(resolvedPath)) {
    throw new Error(`LanceDB path not found: ${resolvedPath}`);
  }
  if (cachedDb && cachedDbPath === resolvedPath) {
    return cachedDb;
  }
  cachedDb = await connect(resolvedPath);
  cachedDbPath = resolvedPath;
  return cachedDb;
}

export async function listMemories(
  options: MemoryListOptions = {},
  dbPath?: string,
): Promise<MemoryListResult> {
  const db = await getDb(dbPath);
  const tableNames = await db.tableNames();
  if (!tableNames.includes(TABLE_NAME)) {
    return { entries: [], total: 0, categories: [], scopes: [] };
  }

  const table = await db.openTable(TABLE_NAME);
  let query = table.query();

  // Filter by category
  if (options.category && options.category !== "all") {
    query = query.where(`category = '${options.category.replace(/'/g, "\\'")}'`);
  }

  // Filter by scope
  if (options.scope && options.scope !== "all") {
    query = query.where(`scope = '${options.scope.replace(/'/g, "\\'")}'`);
  }

  // Text search filter
  if (options.search) {
    const escaped = options.search.replace(/'/g, "\\'");
    query = query.where(`text LIKE '%${escaped}%'`);
  }

  const limit = Math.min(options.limit || 50, 200);
  const offset = options.offset || 0;

  // Get total count (full table for now; LanceDB doesn't support count with filter efficiently)
  const allRows = await table.query().select(["id"]).limit(10000).toArray();
  const allFiltered = await query.select(["id"]).limit(10000).toArray();

  // Fetch actual data
  const rows = await query
    .select(["id", "text", "category", "scope", "importance", "timestamp", "metadata"])
    .limit(limit)
    .offset(offset)
    .toArray();

  const entries: MemoryEntryRow[] = rows.map((row: Record<string, unknown>) => ({
    id: String(row.id ?? ""),
    text: String(row.text ?? ""),
    category: String(row.category ?? "other"),
    scope: String((row.scope as string) ?? "global"),
    importance: Number(row.importance ?? 0),
    timestamp: Number(row.timestamp ?? 0),
    metadata: row.metadata != null ? String(row.metadata) : undefined,
  }));

  // Get distinct categories and scopes from all rows
  const allData = await table
    .query()
    .select(["category", "scope"])
    .limit(10000)
    .toArray();

  const categories = [...new Set(allData.map((r: Record<string, unknown>) => String(r.category ?? "other")))].sort();
  const scopes = [...new Set(allData.map((r: Record<string, unknown>) => String((r as Record<string, unknown>).scope ?? "global")))].sort();

  return {
    entries,
    total: allFiltered.length,
    categories,
    scopes,
  };
}

export async function getMemoryById(
  id: string,
  dbPath?: string,
): Promise<MemoryEntryRow | null> {
  const db = await getDb(dbPath);
  const tableNames = await db.tableNames();
  if (!tableNames.includes(TABLE_NAME)) return null;

  const table = await db.openTable(TABLE_NAME);
  const rows = await table
    .query()
    .where(`id = '${id.replace(/'/g, "\\'")}'`)
    .select(["id", "text", "category", "scope", "importance", "timestamp", "metadata"])
    .limit(1)
    .toArray();

  if (rows.length === 0) return null;
  const row = rows[0] as Record<string, unknown>;
  return {
    id: String(row.id ?? ""),
    text: String(row.text ?? ""),
    category: String(row.category ?? "other"),
    scope: String(row.scope ?? "global"),
    importance: Number(row.importance ?? 0),
    timestamp: Number(row.timestamp ?? 0),
    metadata: row.metadata != null ? String(row.metadata) : undefined,
  };
}

export async function getMemoryStats(dbPath?: string): Promise<{
  total: number;
  byCategory: Record<string, number>;
  byScope: Record<string, number>;
  dbExists: boolean;
  tableExists: boolean;
}> {
  const resolvedPath = dbPath || DEFAULT_DB_PATH;
  const dbExists = fs.existsSync(resolvedPath);

  if (!dbExists) {
    return { total: 0, byCategory: {}, byScope: {}, dbExists: false, tableExists: false };
  }

  const db = await connect(resolvedPath);
  const tableNames = await db.tableNames();
  const tableExists = tableNames.includes(TABLE_NAME);

  if (!tableExists) {
    return { total: 0, byCategory: {}, byScope: {}, dbExists: true, tableExists: false };
  }

  const table = await db.openTable(TABLE_NAME);
  const rows = await table.query().select(["category", "scope"]).limit(10000).toArray();

  const byCategory: Record<string, number> = {};
  const byScope: Record<string, number> = {};
  for (const row of rows) {
    const cat = String((row as Record<string, unknown>).category ?? "other");
    const scope = String((row as Record<string, unknown>).scope ?? "global");
    byCategory[cat] = (byCategory[cat] || 0) + 1;
    byScope[scope] = (byScope[scope] || 0) + 1;
  }

  return { total: rows.length, byCategory, byScope, dbExists: true, tableExists: true };
}
