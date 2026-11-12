import { Module } from '@nestjs/common';
import { Database } from './infra/database';
import { WalletController, WagerController, HealthController, ProviderTransactionController } from './http/controllers';
import { WagerService } from './services/wager.service';
import { WalletService } from './services/wallet.service';
import { QueueWorkers } from './workers/queue.workers';
import { Metrics } from './infra/metrics';
import { NoopAuthGuard } from './auth/noop-auth.guard';
import { PROVIDER_IDENTITY_PORT } from './auth/provider-identity.port';

@Module({ controllers: [WalletController, WagerController, HealthController, ProviderTransactionController], providers: [Database, WagerService, WalletService, QueueWorkers, Metrics, NoopAuthGuard, {provide:PROVIDER_IDENTITY_PORT,useValue:{validate:async()=>true}}] })
export class AppModule {}
