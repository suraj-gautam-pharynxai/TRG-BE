import { Injectable, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Pool } from 'pg';

@Injectable()
export class DatabaseService implements OnModuleInit {
  private pool!: Pool;

  constructor(private readonly configService: ConfigService) { }

  async onModuleInit(): Promise<void> {
    this.pool = new Pool({
      connectionString: this.configService.get<string>('DATABASE_URL'),
    });

    await this.ensurePgVector();
    await this.ensurePgCrypto();
    await this.ensureSchema();
  }

  async query<T = unknown>(text: string, params?: unknown[]): Promise<{ rows: T[] }> {
    return this.pool.query<T>(text, params as any);
  }

  private async ensurePgVector(): Promise<void> {
    await this.pool.query('CREATE EXTENSION IF NOT EXISTS vector');
  }

  private async ensurePgCrypto(): Promise<void> {
    await this.pool.query('CREATE EXTENSION IF NOT EXISTS pgcrypto');
  }

  private async ensureSchema(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS dashboard_chunks (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        source TEXT NOT NULL,
        content TEXT NOT NULL,
        embedding vector(1536) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    // // Check if graph_data table exists with old schema
    // const tableExists = await this.pool.query(`
    //   SELECT EXISTS (
    //     SELECT FROM information_schema.tables 
    //     WHERE table_name = 'graph_data'
    //   );
    // `);

    // if (tableExists.rows[0].exists) {
    //   // Check if old columns exist
    //   const hasOldColumns = await this.pool.query(`
    //     SELECT EXISTS (
    //       SELECT FROM information_schema.columns 
    //       WHERE table_name = 'graph_data' AND column_name = 'metric'
    //     );
    //   `);

    //   if (hasOldColumns.rows[0].exists) {
    //     // Drop old table and recreate with new schema
    //     await this.pool.query('DROP TABLE graph_data CASCADE');
    //   }
    // }

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS graph_data (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        source TEXT NOT NULL,
        table_data JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS chat_history (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      query TEXT NOT NULL,
      response TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    await this.pool.query(`
      CREATE INDEX IF NOT EXISTS dashboard_chunks_embedding_idx
      ON dashboard_chunks USING ivfflat (embedding vector_cosine_ops);
    `);

    await this.pool.query(`
      CREATE INDEX IF NOT EXISTS graph_data_source_idx
      ON graph_data (source);
    `);
  }

  async deleteBySource(source: string): Promise<void> {
    await this.pool.query('DELETE FROM dashboard_chunks WHERE source = $1', [source]);
    await this.pool.query('DELETE FROM graph_data WHERE source = $1', [source]);
  }
}


