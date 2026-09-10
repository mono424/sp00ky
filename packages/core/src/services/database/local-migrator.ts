import type { Logger} from '../logger/index';
import { createLogger } from '../logger/index';
import type { LocalStore } from './cache-engine';

export interface SchemaRecord {
  hash: string;
  created_at: string;
}

export const sha1 = async (str: string): Promise<string> => {
  const enc = new TextEncoder();
  const hash = await crypto.subtle.digest('SHA-1', enc.encode(str));
  return Array.from(new Uint8Array(hash))
    .map((v) => v.toString(16).padStart(2, '0'))
    .join('');
};

export class LocalMigrator {
  private logger: Logger;

  constructor(
    private localDb: LocalStore,
    logger: Logger
  ) {
    this.logger = logger.child({ service: 'LocalMigrator' });
    void (logger?.child({ service: 'LocalMigrator' }) ??
      createLogger('info').child({ service: 'LocalMigrator' }));
  }

  async provision(schemaSurql: string): Promise<void> {
    const hash = await sha1(schemaSurql);

    const { database } = this.localDb.getConfig();

    if (await this.isSchemaUpToDate(hash)) {
      this.logger.info(
        { Category: 'sp00ky-client::LocalMigrator::provision' },
        '[Provisioning] Schema is up to date, skipping migration'
      );
      return;
    }

    await this.recreateDatabase(database);

    const systemSchema = `
      DEFINE TABLE IF NOT EXISTS _00_stream_processor_state SCHEMALESS PERMISSIONS FOR select, create, update, delete WHERE true;
      DEFINE TABLE IF NOT EXISTS _00_view SCHEMALESS PERMISSIONS FOR select, create, update, delete WHERE true;
      DEFINE TABLE IF NOT EXISTS _00_window SCHEMALESS PERMISSIONS FOR select, create, update, delete WHERE true;
      DEFINE TABLE IF NOT EXISTS _00_failed_mutations SCHEMALESS PERMISSIONS FOR select, create, update, delete WHERE true;
      DEFINE TABLE IF NOT EXISTS _00_schema SCHEMALESS PERMISSIONS FOR select, create, update, delete WHERE true;
      DEFINE TABLE IF NOT EXISTS _00_pending_mutations SCHEMALESS PERMISSIONS FOR select, create, update, delete WHERE true;
      DEFINE TABLE IF NOT EXISTS _00_blob SCHEMALESS PERMISSIONS FOR select, create, update, delete WHERE true;
    `;
    const fullSchema = schemaSurql + '\n' + systemSchema;

    const statements = this.splitStatements(fullSchema);

    for (let i = 0; i < statements.length; i++) {
      const statement = statements[i];
      // Strip comments to check if it's an index definition
      const cleanStatement = statement.replace(/--.*/g, '').trim();

      // SKIP INDEXES: WASM engine hangs on DEFINE INDEX (confirmed)
      if (cleanStatement.toUpperCase().startsWith('DEFINE INDEX')) {
        this.logger.warn(
          { Category: 'sp00ky-client::LocalMigrator::provision' },
          `[Provisioning] Skipping index definition (WASM hang avoidance): ${cleanStatement.substring(0, 50)}...`
        );
        continue;
      }

      try {
        this.logger.info(
          { Category: 'sp00ky-client::LocalMigrator::provision' },
          `[Provisioning] (${i + 1}/${statements.length}) Executing: ${statement.substring(0, 50)}...`
        );
        await this.localDb.queryUngated(statement);
        this.logger.info(
          { Category: 'sp00ky-client::LocalMigrator::provision' },
          `[Provisioning] (${i + 1}/${statements.length}) Done`
        );
      } catch (e) {
        this.logger.error(
          { Category: 'sp00ky-client::LocalMigrator::provision' },
          `[Provisioning] (${i + 1}/${statements.length}) Error executing statement: ${statement}`
        );
        throw e;
      }
    }

    await this.createHashRecord(hash);
  }

  private async isSchemaUpToDate(hash: string): Promise<boolean> {
    try {
      const [lastSchemaRecord] = await this.localDb.queryUngated<any>(
        `SELECT hash, created_at FROM ONLY _00_schema ORDER BY created_at DESC LIMIT 1;`
      );
      return lastSchemaRecord?.hash === hash;
    } catch (_error) {
      return false;
    }
  }

  private async recreateDatabase(database: string) {
    try {
      await this.localDb.queryUngated(`DEFINE DATABASE _00_temp;`);
    } catch (_e) {
      // Ignore if exists
    }

    try {
      await this.localDb.queryUngated(`
        USE DB _00_temp;
        REMOVE DATABASE ${database};
      `);
    } catch (_e) {
      // Ignore error if database doesn't exist
    }

    await this.localDb.queryUngated(`
      DEFINE DATABASE ${database};
      USE DB ${database};
    `);
  }

  private splitStatements(schema: string): string[] {
    const statements: string[] = [];
    let current = '';
    let depth = 0;
    let inQuote = false;
    let quoteChar = '';
    let inComment = false;

    for (let i = 0; i < schema.length; i++) {
      const char = schema[i];
      const nextChar = schema[i + 1];

      // Handle Comments
      if (inComment) {
        current += char;
        if (char === '\n') {
          inComment = false;
        }
        continue;
      }

      // Start of comment
      if (!inQuote && char === '-' && nextChar === '-') {
        inComment = true;
        current += char;
        continue;
      }

      if (inQuote) {
        current += char;
        if (char === quoteChar && schema[i - 1] !== '\\') {
          inQuote = false;
        }
        continue;
      }

      if (char === '"' || char === "'") {
        inQuote = true;
        quoteChar = char;
        current += char;
        continue;
      }

      if (char === '{') {
        depth++;
        current += char;
        continue;
      }

      if (char === '}') {
        depth--;
        current += char;
        continue;
      }

      if (char === ';' && depth === 0) {
        if (current.trim().length > 0) {
          statements.push(current.trim());
        }
        current = '';
        continue;
      }

      current += char;
    }

    if (current.trim().length > 0) {
      statements.push(current.trim());
    }

    return statements;
  }

  private async createHashRecord(hash: string) {
    await this.localDb.queryUngated(
      `UPSERT _00_schema SET hash = $hash, created_at = time::now() WHERE hash = $hash;`,
      { hash }
    );
  }
}
