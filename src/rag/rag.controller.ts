import { Controller, Get, Post, Query, UploadedFile, UseInterceptors } from '@nestjs/common';
import { RagService } from './rag.service';
import { FileInterceptor } from '@nestjs/platform-express';
import * as multer from 'multer';

@Controller('rag')
export class RagController {
  constructor(private readonly ragService: RagService) {}

  @Get('query')
  async query(
    @Query('q') question: string,
    @Query('k') k: string = '5',
    @Query('source') source?: string,
  ): Promise<{ answer: string; contexts: Array<{ id: string; source: string; content: string; score: number }> }> {
    return this.ragService.query(question, Number(k), source);
  }

  @Get('data')
  async getData(@Query('source') source?: string): Promise<{ data: Array<{ id: string; source: string; table_data: any; created_at: string }> }> {
    return this.ragService.getData(source);
  }

  @Post('ingest/file')
  @UseInterceptors(FileInterceptor('file', { storage: multer.memoryStorage() }))
  async ingestFile(
    @UploadedFile() file: Express.Multer.File,
    @Query('source') source: string,
  ): Promise<{ inserted: number; graphDataInserted: number }> {
    return this.ragService.ingestFile(source ?? file.originalname, file);
  }
}


