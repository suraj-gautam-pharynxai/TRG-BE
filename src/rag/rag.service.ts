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
    // await this.db.deleteBySource(source);

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

  async query(question: string, k = 5): Promise<{
    answer: string;
    contexts: Array<{ id: string; content: string; score: number }>;
  }> {
    const qEmbedding = await this.createEmbedding(question);
    console.log({})
    // 🔹 Step 1: Semantic search
    const { rows: semanticRows } = await this.db.query<{
      id: string;
      content: string;
      score: number;
    }>(
      `SELECT id, content, 1 - (embedding <=> $1::vector) AS score
   FROM dashboard_chunks
   ORDER BY score DESC
   LIMIT $2`,
      [toSql(qEmbedding), 5],
    );


    console.log({ semanticRows });

    // 🔹 Step 2: Keyword fallback (always run if the Q has tokens like "AEUUU")
    const keywordRows = await this.keywordFallbackSearch(question, k);
    console.log({ keywordRows });

    // 🔹 Step 3: Merge both sets (avoid duplicates)
    const combinedMap = new Map<string, { id: string; content: string; score: number }>();
    for (const row of semanticRows) combinedMap.set(row.id, row);
    for (const row of keywordRows) {
      if (!combinedMap.has(row.id)) {
        combinedMap.set(row.id, { ...row, score: 0.75 }); // give keyword hits medium score
      }
    }

    const finalRows = Array.from(combinedMap.values())
      .sort((a, b) => b.score - a.score)
      .slice(0, k);

    console.log({ finalRows });

    // 🔹 Step 4: Build context
    const contextText = finalRows.map((r) => r.content).join('\n---\n');

    // fetch last 3 query-response pairs
    const chatHistory = await this.getChatHistory(1, 3);
    const historyContext = chatHistory
      .map((h: any) => `Q: ${h.query}\nA: ${h.response}`)
      .join("\n---\n");

    // 🔹 Step 5: Generate answer
    const answer = await this.generateAnswer(question, `${historyContext}\n---\n${contextText}`);

    await this.saveChatHistory(question, answer);

    return { answer, contexts: finalRows };
  }

  private async createEmbedding(input: string): Promise<number[]> {
    const res = await this.openai.embeddings.create({
      model: 'text-embedding-ada-002',
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
            'You are a data analyst. Use both the chat history and the provided context from company dashboard. If uncertain, say you do not know.',
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

  // private async ingestRows(
  //   source: string,
  //   rows: Array<Record<string, unknown>>,
  //   prefix?: string,
  // ): Promise<number> {
  //   if (!rows || rows.length === 0) return 0;
  //   const headers = Object.keys(rows[0]);
  //   let inserted = 0;
  //   for (const row of rows) {
  //     const parts: string[] = [];
  //     for (const h of headers) {
  //       const value = (row as any)[h];
  //       parts.push(`${h}: ${String(value)}`);
  //     }
  //     const content = (prefix ? `${prefix} \n` : '') + parts.join('; ');
  //     inserted += await this.insertChunk(source, content);
  //   }
  //   return inserted;
  // }


  private async ingestRows(
    source: string,
    rows: Array<Record<string, unknown>>,
    prefix?: string,
  ): Promise<number> {
    if (!rows || rows.length === 0) return 0;

    const headers = Object.keys(rows[0]);
    let inserted = 0;

    // ✅ Whole sheet as one chunk
    {
      const sheetParts = rows.map((row) =>
        headers.map((h) => `${h}: ${String((row as any)[h])}`).join('; ')
      );
      const sheetContent = (prefix ? `${prefix}\n` : '') + sheetParts.join('\n');
      inserted += await this.insertChunk(source, sheetContent);
    }

    // ✅ Each row as one chunk (keeps relationships intact)
    // for (const row of rows) {
    //   const rowParts = headers.map((h) => `${h}: ${String((row as any)[h])}`);
    //   const rowContent = (prefix ? `${prefix}\n` : '') + rowParts.join('; ');
    //   inserted += await this.insertChunk(source, rowContent);
    // }

    // ✅ Each column as one chunk (for trend/summary queries)
    for (const h of headers) {
      const values = rows.map((r) => String((r as any)[h] ?? ''));
      const colContent =
        (prefix ? `${prefix}\n` : '') + `Column: ${h}\n` + values.join('\n');
      inserted += await this.insertChunk(source, colContent);
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
  ): Promise<Array<{ id: string; content: string }>> {
    const normalized = question.toLowerCase();

    // tokenize question
    const stopwords = new Set([
      'what', 'is', 'the', 'of', 'for', 'a', 'an', 'and', 'or', 'to', 'me', 'data', 'give', 'provide', 'show', 'please', 'about'
    ]);
    const rawTokens = normalized
      .replace(/[^a-z0-9()\s.-]/g, ' ')
      .split(/\s+/)
      .filter(Boolean)
      .filter((t) => !stopwords.has(t))
      .filter((t) => t.length >= 2);

    // Build ILIKE conditions
    const conditions: string[] = [];
    const params: any[] = [];
    let paramIndex = 1;

    for (const token of rawTokens) {
      conditions.push(`content ILIKE $${paramIndex++}`);
      params.push(`%${token}%`);
    }

    const where = conditions.length ? conditions.join(' AND ') : '';

    const sql = `SELECT id, content
               FROM dashboard_chunks
               ${where ? 'WHERE ' + where : ''}
               ORDER BY created_at DESC
               LIMIT ${k}`;

    const { rows } = await this.db.query<{ id: string; content: string }>(sql, params);
    return rows;
  }

  private async saveChatHistory(query: string, response: string) {
    await this.db.query(
      `INSERT INTO chat_history (query, response) VALUES ($1, $2)`,
      [query, response]
    );
  }

  async getChatHistory(page: number = 1, limit: number = 10) {
    const offset = (page - 1) * limit;

    const result = await this.db.query(
      `SELECT id, query, response, created_at
     FROM chat_history
     ORDER BY created_at DESC
     LIMIT $1 OFFSET $2`,
      [limit, offset]
    );

    return result.rows;
  }
}




{
  finalRows: [
    {
      id: 'c300f707-122c-42e7-8df1-f97ca144b9bb',
      content: 'Column: Sum of Risk Contribution\n' +
        '30.80%\n' +
        '11.80%\n' +
        '10.00%\n' +
        '7.00%\n' +
        '6.50%\n' +
        '5.60%\n' +
        '5.40%\n' +
        '4.80%\n' +
        '3.90%\n' +
        '3.20%\n' +
        '2.10%\n' +
        '1.60%\n' +
        '1.40%\n' +
        '1.40%\n' +
        '1.40%\n' +
        '1.10%\n' +
        '0.90%\n' +
        '0.70%\n' +
        '0.30%\n' +
        '0.00%\n' +
        '99.90%',
      score: 0.8255897268301426
    },
    {
      id: 'd841982a-1524-43f0-80d5-0f228c191d7a',
      content: 'Securities: AEUUU; Sum of Risk Contribution: 30.80%\n' +
        'Securities: NUHGZ; Sum of Risk Contribution: 11.80%\n' +
        'Securities: CSTNL; Sum of Risk Contribution: 10.00%\n' +
        'Securities: SSDRF; Sum of Risk Contribution: 7.00%\n' +
        'Securities: JPMMZ; Sum of Risk Contribution: 6.50%\n' +
        'Securities: AGACZ; Sum of Risk Contribution: 5.60%\n' +
        'Securities: MFMER; Sum of Risk Contribution: 5.40%\n' +
        'Securities: TIESI; Sum of Risk Contribution: 4.80%\n' +
        'Securities: SCHHH; Sum of Risk Contribution: 3.90%\n' +
        'Securities: IHHSF; Sum of Risk Contribution: 3.20%\n' +
        'Securities: MOTTZ; Sum of Risk Contribution: 2.10%\n' +
        'Securities: CACCZ; Sum of Risk Contribution: 1.60%\n' +
        'Securities: LODUT; Sum of Risk Contribution: 1.40%\n' +
        'Securities: ISHVF; Sum of Risk Contribution: 1.40%\n' +
        'Securities: PGLAZ; Sum of Risk Contribution: 1.40%\n' +
        'Securities: IMSCF; Sum of Risk Contribution: 1.10%\n' +
        'Securities: LODEZ; Sum of Risk Contribution: 0.90%\n' +
        'Securities: EUEIC; Sum of Risk Contribution: 0.70%\n' +
        'Securities: LOMIZ; Sum of Risk Contribution: 0.30%\n' +
        'Securities: USD.CCY; Sum of Risk Contribution: 0.00%\n' +
        'Securities: Grand Total; Sum of Risk Contribution: 99.90%',
      score: 0.8001854538807016
    },
    {
      id: 'b239c015-ccb7-490a-b392-1cc73ef1bf7a',
      content: 'Column: Securities\n' +
        'AEUUU\n' +
        'NUHGZ\n' +
        'CSTNL\n' +
        'SSDRF\n' +
        'JPMMZ\n' +
        'AGACZ\n' +
        'MFMER\n' +
        'TIESI\n' +
        'SCHHH\n' +
        'IHHSF\n' +
        'MOTTZ\n' +
        'CACCZ\n' +
        'LODUT\n' +
        'ISHVF\n' +
        'PGLAZ\n' +
        'IMSCF\n' +
        'LODEZ\n' +
        'EUEIC\n' +
        'LOMIZ\n' +
        'USD.CCY\n' +
        'Grand Total',
      score: 0.7389190417206084
    }
  ]
}