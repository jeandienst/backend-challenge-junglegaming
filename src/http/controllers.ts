import { Body, Controller, Get, Headers, Header, HttpCode, HttpStatus, NotFoundException, Param, ParseUUIDPipe, Post, Query, ServiceUnavailableException } from '@nestjs/common';
import { WalletService } from '../services/wallet.service';
import { WagerService } from '../services/wager.service';
import { Database } from '../infra/database';
import { QueueWorkers } from '../workers/queue.workers';
import { Metrics } from '../infra/metrics';

@Controller('wallets')
export class WalletController {
  constructor(private readonly wallets:WalletService){}
  @Post() @HttpCode(HttpStatus.CREATED) create(@Body() body:any){return this.wallets.create(body?.playerId,body?.initialBalance);}
  @Get(':walletId') get(@Param('walletId',ParseUUIDPipe) id:string){return this.wallets.get(id);}
  @Get(':walletId/ledger') ledger(@Param('walletId',ParseUUIDPipe) id:string,@Query('cursor') cursor?:string,@Query('limit') limit?:string){const parsed=limit===undefined?50:Number(limit);return this.wallets.ledger(id,cursor,parsed);}
  @Post(':walletId/reconciliation') reconcile(@Param('walletId',ParseUUIDPipe) id:string){return this.wallets.reconcile(id);}
}
@Controller('wagering/transactions')
export class WagerController {
  constructor(private readonly wagers:WagerService){}
  @Post() @HttpCode(HttpStatus.ACCEPTED) submit(@Body() body:unknown,@Headers('idempotency-key') key:string){return this.wagers.submit(body,key);}
  @Get(':transactionId') async get(@Param('transactionId',ParseUUIDPipe) id:string){const r=await this.wagers.get(id);if(!r) throw new NotFoundException({code:'TRANSACTION_NOT_FOUND'});return r;}
}
@Controller('providers/:providerId/wagering/transactions')
export class ProviderTransactionController {
  constructor(private readonly wagers:WagerService){}
  @Get(':externalTransactionId') async get(@Param('providerId') p:string,@Param('externalTransactionId') id:string){const r=await this.wagers.getByExternal(p,id);if(!r)throw new NotFoundException({code:'TRANSACTION_NOT_FOUND'});return r;}
}
@Controller('health')
export class HealthController {
  constructor(private readonly db:Database,private readonly workers:QueueWorkers,private readonly metrics:Metrics){}
  @Get('live') live(){return {status:'ok'};}
  @Get('ready') async ready(){try{await this.db.query('select 1');await this.workers.ready();return {status:'ready'};}catch{throw new ServiceUnavailableException({status:'not_ready'});}}
  @Get('metrics') @Header('Content-Type','text/plain; version=0.0.4; charset=utf-8') metricsEndpoint(){return this.metrics.render();}
}
