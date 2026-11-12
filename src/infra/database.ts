import { Injectable, OnModuleDestroy, OnModuleInit, ServiceUnavailableException } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import config from '../../mikro-orm.config';

/** MikroORM/Knex binds raw SQL with `?` placeholders, not node-postgres `$1` placeholders. */
function bindSql(sql: string, params: unknown[]): { sql: string; params: unknown[] } {
  const bindings: unknown[] = [];
  const query = sql.replace(/\$(\d+)/g, (_placeholder, index: string) => {
    const position = Number(index) - 1;
    if (position < 0 || position >= params.length) {
      throw new RangeError(`SQL placeholder $${index} has no corresponding parameter`);
    }
    const value = params[position];
    // MikroORM's SQL formatter expands JS arrays as comma-separated SQL values.
    // Preserve PostgreSQL `ANY($n::type[])` semantics by binding one array literal.
    bindings.push(Array.isArray(value)
      ? `{${value.map(item => item === null ? 'NULL' : `"${String(item).replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`).join(',')}}`
      : value);
    return '?';
  });
  return { sql: query, params: bindings };
}

@Injectable()
export class Database implements OnModuleInit, OnModuleDestroy {
  orm!: MikroORM;
  async onModuleInit(): Promise<void> { this.orm = await MikroORM.init({ ...config, clientUrl: process.env.DATABASE_URL ?? 'postgresql://wager:wager@localhost:5432/wagering' }); }
  async onModuleDestroy(): Promise<void> { await this.orm?.close(true); }
  async query<T extends object = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<T[]> {
    try { const bound = bindSql(sql, params); return await this.orm.em.execute<T[]>(bound.sql, bound.params, 'all'); } catch(error) { this.mapInfrastructureFailure(error); }
  }
  async tx<T>(fn: (query: <R extends object = Record<string, unknown>>(sql: string, params?: unknown[]) => Promise<R[]>) => Promise<T>): Promise<T> {
    try {
      return await this.orm.em.transactional(async em => fn(async <R extends object = Record<string, unknown>>(sql: string, params: unknown[] = []) => {
        try { const bound = bindSql(sql, params); return await em.execute<R[]>(bound.sql, bound.params, 'all'); } catch(error) { this.mapInfrastructureFailure(error); }
      }));
    } catch(error) { this.mapInfrastructureFailure(error); }
  }
  private mapInfrastructureFailure(error:unknown):never{
    const e=error as {code?:string;cause?:{code?:string}};const code=e?.code??e?.cause?.code;
    if(code?.startsWith('08')||['57P01','57P02','57P03','53300','ECONNREFUSED','ETIMEDOUT','EPIPE','ECONNRESET','ENOTFOUND'].includes(code??''))throw new ServiceUnavailableException({code:'TRANSIENT_INFRASTRUCTURE'});
    throw error;
  }
}
