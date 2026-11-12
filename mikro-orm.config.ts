import { defineConfig } from '@mikro-orm/postgresql';
import { Migrator } from '@mikro-orm/migrations';

export default defineConfig({
  clientUrl: process.env.DATABASE_URL ?? 'postgresql://wager:wager@localhost:5432/wagering',
  extensions: [Migrator],
  entities: [],
  // The schema is managed by SQL migrations and the application uses pg directly.
  // MikroORM still runs the migration provider without mapped entity classes.
  discovery: { warnWhenNoEntities: false },
  migrations: { path: './migrations', pathTs: './migrations', glob: '!(*.d).{js,ts}' },
  debug: false,
});
