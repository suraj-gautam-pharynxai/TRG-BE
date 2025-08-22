import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import OpenAI from 'openai';
import { DatabaseService } from '../database/database.service';
import { toSql } from 'pgvector';
import { parse as parseCsv } from 'csv-parse/sync';
import * as XLSX from 'xlsx';

function splitIntoSentences(text: string): string[] {
  return text
    .split(/(?<=[.!?])\s+/)
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

function chunkText(text: string, targetTokens = 120): string[] {
  const sentences = splitIntoSentences(text);
  const chunks: string[] = [];
  let current = '';
  for (const sentence of sentences) {
    if ((current + ' ' + sentence).split(/\s+/).length > targetTokens) {
      if (current.trim().length > 0) chunks.push(current.trim());
      current = sentence;
    } else {
      current = current.length ? `${current} ${sentence}` : sentence;
    }
  }
  if (current.trim().length > 0) chunks.push(current.trim());
  return chunks;
}

@Injectable()
export class RagService {
  private readonly openai: OpenAI;

  constructor(
    private readonly db: DatabaseService,
    private readonly config: ConfigService,
  ) {
    this.openai = new OpenAI({ apiKey: this.config.get<string>('OPENAI_API_KEY') });
  }

  private async ingestText(source: string, content: string): Promise<{ inserted: number }> {
    const chunks = chunkText(content, 120);
    let inserted = 0;
    for (const chunk of chunks) {
      inserted += await this.insertChunk(source, chunk);
    }
    return { inserted };
  }


  async ingestFile(source: string, file: Express.Multer.File): Promise<{ inserted: number; graphDataInserted: number }> {
    // Delete old entries for this source
    await this.db.deleteBySource(source);

    const mime = file.mimetype;
    const buf = file.buffer;
    // CSV
    if (mime === 'text/csv' || file.originalname.toLowerCase().endsWith('.csv')) {
      const records: Array<Record<string, string>> = parseCsv(buf.toString('utf8'), {
        columns: true,
        skip_empty_lines: true,
        bom: true,
      });
      const inserted = await this.ingestRows(source, records);
      const graphDataInserted = await this.ingestGraphData(source, records);
      return { inserted, graphDataInserted };
    }
    // XLS/XLSX
    if (
      mime ===
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' ||
      mime === 'application/vnd.ms-excel' ||
      file.originalname.toLowerCase().endsWith('.xlsx') ||
      file.originalname.toLowerCase().endsWith('.xls')
    ) {
      const workbook = XLSX.read(buf, { type: 'buffer' });
      const sheetNames = workbook.SheetNames;
      let inserted = 0;
      let graphDataInserted = 0;
      for (const name of sheetNames) {
        const sheet = workbook.Sheets[name];
        const json = XLSX.utils.sheet_to_json<Record<string, unknown>>(sheet, { defval: '' });
        inserted += await this.ingestRows(source, json, `Sheet: ${name}`);
        graphDataInserted += await this.ingestGraphData(source, json);
      }
      return { inserted, graphDataInserted };
    }
    // Fallback: treat as text
    const result = await this.ingestText(source, buf.toString('utf8'));
    return { inserted: result.inserted, graphDataInserted: 0 };
  }

  async query(question: string, k = 5, source?: string): Promise<{
    answer: string;
    contexts: Array<{ id: string; source: string; content: string; score: number }>;
  }> {
    const qEmbedding = await this.createEmbedding(question);
    const { rows } = await this.db.query<{
      id: string;
      source: string;
      content: string;
      score: number;
    }>(
      `SELECT id, source, content, 1 - (embedding <=> $1::vector) AS score
       FROM dashboard_chunks
       ${source ? 'WHERE source = $3' : ''}
       ORDER BY embedding <=> $1::vector
       LIMIT $2`,
      source ? [toSql(qEmbedding), k, source] : [toSql(qEmbedding), k],
    );

    if (rows.length === 0) {
      const fallbackRows = await this.keywordFallbackSearch(question, k, source);
      const contextTextFb = fallbackRows.map((r) => r.content).join('\n---\n');
      const answerFb = await this.generateAnswer(question, contextTextFb);
      return { answer: answerFb, contexts: fallbackRows.map((r) => ({ ...r, score: 0 })) };
    }

    const contextText = rows.map((r) => r.content).join('\n---\n');
    const answer = await this.generateAnswer(question, contextText);
    await this.saveChatHistory(question, answer)
    return { answer, contexts: rows };
  }

  private async createEmbedding(input: string): Promise<number[]> {
    const res = await this.openai.embeddings.create({
      model: 'text-embedding-3-small',
      input,
    });
    return res.data[0].embedding as unknown as number[];
  }

  private async generateAnswer(question: string, context: string): Promise<string> {
    const res = await this.openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        {
          role: 'system',
          content:
            'You are a data analyst. Answer based ONLY on the provided context from company dashboard. If uncertain, say you do not know.',
        },
        { role: 'user', content: `Context:\n${context}\n\nQuestion: ${question}` },
      ],
      temperature: 0,
    });
    return res.choices[0]?.message?.content ?? '';
  }

  private async insertChunk(source: string, content: string): Promise<number> {
    const embedding = await this.createEmbedding(content);
    await this.db.query(
      `INSERT INTO dashboard_chunks (source, content, embedding) VALUES ($1, $2, $3)`,
      [source, content, toSql(embedding)],
    );
    return 1;
  }

  private async ingestRows(
    source: string,
    rows: Array<Record<string, unknown>>,
    prefix?: string,
  ): Promise<number> {
    if (!rows || rows.length === 0) return 0;
    const headers = Object.keys(rows[0]);
    let inserted = 0;
    for (const row of rows) {
      const parts: string[] = [];
      for (const h of headers) {
        const value = (row as any)[h];
        parts.push(`${h}: ${String(value)}`);
      }
      const content = (prefix ? `${prefix} \n` : '') + parts.join('; ');
      inserted += await this.insertChunk(source, content);
    }
    return inserted;
  }

  async getData(source?: string): Promise<{ data: Array<{ id: string; source: string; table_data: any; created_at: string }> }> {
    const query = source
      ? 'SELECT * FROM graph_data WHERE source = $1 ORDER BY created_at DESC'
      : 'SELECT * FROM graph_data ORDER BY created_at DESC';
    const params = source ? [source] : [];
    const { rows } = await this.db.query<{ id: string; source: string; table_data: any; created_at: string }>(query, params);
    return { data: rows };
  }

  private async ingestGraphData(
    source: string,
    rows: Array<Record<string, unknown>>,
  ): Promise<number> {
    if (!rows || rows.length === 0) return 0;

    // Store entire table data as JSONB
    await this.db.query(
      `INSERT INTO graph_data (source, table_data) VALUES ($1, $2)`,
      [source, JSON.stringify(rows)],
    );
    return 1;
  }

  private tabularToText(rows: Array<Record<string, unknown>>): string {
    if (!rows || rows.length === 0) return '';
    const headers = Object.keys(rows[0]);
    const lines: string[] = [];
    lines.push(headers.join(' | '));
    for (const row of rows) {
      lines.push(headers.map((h) => String((row as any)[h] ?? '')).join(' | '));
    }
    return lines.join('\n');
  }

  private async keywordFallbackSearch(
    question: string,
    k: number,
    source?: string,
  ): Promise<Array<{ id: string; source: string; content: string }>> {
    const normalized = question.toLowerCase();
    const specialPhrases: string[] = [];
    if (normalized.includes('nifty 50')) {
      specialPhrases.push('nifty 50', 'nifty 50 (benchmark)');
    }

    const stopwords = new Set([
      'what', 'is', 'the', 'of', 'for', 'a', 'an', 'and', 'or', 'to', 'me', 'data', 'give', 'provide', 'show', 'please', 'about'
    ]);
    const rawTokens = normalized
      .replace(/[^a-z0-9()\s.-]/g, ' ')
      .split(/\s+/)
      .filter(Boolean)
      .filter((t) => !stopwords.has(t))
      .filter((t) => t.length >= 2);

    // Build ILIKE conditions requiring all tokens to appear
    const conditions: string[] = [];
    const params: any[] = [];
    let paramIndex = 1;

    for (const token of rawTokens) {
      conditions.push(`content ILIKE $${paramIndex++}`);
      params.push(`%${token}%`);
    }

    for (const phrase of specialPhrases) {
      conditions.push(`content ILIKE $${paramIndex++}`);
      params.push(`%${phrase}%`);
    }

    let where = conditions.length ? conditions.join(' AND ') : '';
    if (source) {
      where = where ? `(${where}) AND source = $${paramIndex++}` : `source = $${paramIndex++}`;
      params.push(source);
    }

    const sql = `SELECT id, source, content
                 FROM dashboard_chunks
                 ${where ? 'WHERE ' + where : ''}
                 ORDER BY created_at DESC
                 LIMIT ${k}`;

    const { rows } = await this.db.query<{ id: string; source: string; content: string }>(sql, params);
    console.log({ rows })
    return rows;
  }

  private async saveChatHistory(query: string, response: string) {
    await this.db.query(
      `INSERT INTO chat_history (query, response) VALUES ($1, $2)`,
      [query, response]
    );
  }
}


