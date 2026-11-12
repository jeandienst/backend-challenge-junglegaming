import { BadRequestException, ConflictException, Injectable, NotFoundException, UnprocessableEntityException } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { Database } from '../infra/database';
import { Money, MoneyProps } from '../domain/money';
import { Wallet, WalletLedgerEntry, LedgerDirection } from '../domain/entities';
import { z } from 'zod';
import { Metrics } from '../infra/metrics';
import { logEvent } from '../infra/logging';
import { IntegrationEvent, WagerTransactionProcessed, WalletBalanceChanged } from '../domain/events';

@Injectable()
export class WalletService {
  constructor(private readonly db: Database,private readonly metrics:Metrics) {}
  async create(playerId: string, initialBalance: MoneyProps) {
    const valid=z.object({playerId:z.string().uuid(),initialBalance:z.object({amount:z.string(),currency:z.string()})}).safeParse({playerId,initialBalance});
    if(!valid.success)throw new UnprocessableEntityException({code:'INVALID_PAYLOAD'});
    let amount:Money;try{amount=Money.from(initialBalance);}catch{throw new UnprocessableEntityException({code:'INVALID_MONEY'});}
    if (amount.isNegative()) throw new Error('INVALID_MONEY');
    return this.db.tx(async q => {
      const [wallet] = await q<{id:string;player_id:string;currency:string;balance:string;version:number}>(`insert into wallets(player_id,currency,balance) values($1,$2,$3) returning *`, [playerId, amount.currency, amount.toJSON().amount]);
      if (!wallet) throw new Error('WALLET_CREATE_FAILED');
      const aggregate=Wallet.open({id:wallet.id,playerId,initialBalance:amount});
      if (amount.isPositive()) {
        const txId = randomUUID();
        await q(`insert into wager_transactions(id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,currency,round_id,game_id,kind,amount,status,processed_at,balance_snapshot) values($1,'internal',$2,$2,$3,$4,$5,$6,'opening','internal','OPENING',$7,'PROCESSED',now(),$7)`, [txId, `opening:${wallet.id}`, '0'.repeat(64), wallet.id, playerId, amount.currency, amount.toJSON().amount]);
        const entry=WalletLedgerEntry.create({id:randomUUID(),walletId:wallet.id,transactionId:txId,direction:LedgerDirection.Credit,money:amount,balanceBefore:Money.zero(amount.currency),balanceAfter:amount});
        await q(`insert into wallet_ledger(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after,created_at) values($1,$2,$3,'CREDIT',$4,$5,0,$4,$6)`, [entry.id, wallet.id, txId, amount.toJSON().amount, amount.currency,entry.createdAt]);
        const context={aggregateId:wallet.id,correlationId:txId,causationId:txId,occurredAt:new Date()};
        await this.enqueue(q,WagerTransactionProcessed.create({eventId:randomUUID(),...context,data:{transactionId:txId,kind:'OPENING',balance:amount.toJSON()}}));
        await this.enqueue(q,WalletBalanceChanged.create({eventId:randomUUID(),...context,data:{walletId:wallet.id,transactionId:txId,direction:LedgerDirection.Credit,money:amount.toJSON(),balanceBefore:Money.zero(amount.currency).toJSON(),balanceAfter:amount.toJSON(),walletVersion:aggregate.version}}));
        this.metrics.inc('wager_transactions_total',{status:'processed',kind:'OPENING'});
      }
      return { id: wallet.id, playerId: wallet.player_id, balance: aggregate.balance.toJSON(), version: aggregate.version };
    }).catch((e: any) => { if (e?.code === '23505') throw new ConflictException({ code: 'WALLET_ALREADY_EXISTS' }); throw e; });
  }
  async get(id: string) {
    const [w] = await this.db.query<any>('select * from wallets where id=$1', [id]);
    if (!w) throw new NotFoundException({ code: 'WALLET_NOT_FOUND' });
    return { id: w.id, playerId: w.player_id, balance: { amount: w.balance, currency: w.currency.trim() }, version: w.version };
  }
  async ledger(id: string, cursor?: string, limit = 50) {
    if(!Number.isInteger(limit))throw new BadRequestException({code:'INVALID_LIMIT'});
    const take = Math.min(Math.max(limit,1),100);
    let createdAt:string|undefined,entryId:string|undefined;
    if(cursor){try{[createdAt,entryId]=Buffer.from(cursor,'base64url').toString().split('|');if(!createdAt||!entryId||Number.isNaN(Date.parse(createdAt))||!z.string().uuid().safeParse(entryId).success)throw new Error();}catch{throw new BadRequestException({code:'INVALID_CURSOR'});}}
    const [wallet] = await this.db.query<any>('select id from wallets where id=$1',[id]);
    if(!wallet)throw new NotFoundException({code:'WALLET_NOT_FOUND'});
    const rows = await this.db.query<any>(`select id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after,created_at from wallet_ledger where wallet_id=$1 and ($2::timestamptz is null or (created_at,id)<($2::timestamptz,$3::uuid)) order by created_at desc,id desc limit $4`, [id, createdAt ?? null, entryId ?? null, take+1]);
    const hasMore = rows.length > take; const items=rows.slice(0,take);
    const last=items.at(-1);
    return {
      items:items.map(row=>({id:row.id,walletId:row.wallet_id,transactionId:row.transaction_id,direction:row.direction,money:{amount:row.amount,currency:row.currency.trim()},balanceBefore:{amount:row.balance_before,currency:row.currency.trim()},balanceAfter:{amount:row.balance_after,currency:row.currency.trim()},createdAt:new Date(row.created_at).toISOString()})),
      nextCursor: hasMore && last ? Buffer.from(`${new Date(last.created_at).toISOString()}|${last.id}`).toString('base64url') : null,
    };
  }
  async reconcile(id: string) {
    return this.db.tx(async q => {
      const [w] = await q<any>('select * from wallets where id=$1 for update', [id]);
      if (!w) throw new NotFoundException({ code: 'WALLET_NOT_FOUND' });
      const [l] = await q<any>(`select coalesce(sum(case when direction='CREDIT' then amount else -amount end),0)::numeric(20,2) as balance,count(*)::int as count from wallet_ledger where wallet_id=$1`, [id]);
      const stored = Money.from({ amount: w.balance, currency: w.currency.trim() });
      const calculatedMoney = Money.from({ amount: l.balance, currency: w.currency.trim() });
      const calculated = calculatedMoney.toJSON().amount; const difference=stored.subtract(calculatedMoney).toJSON().amount;
      const consistent=difference==='0.00';
      if(!consistent){this.metrics.inc('wager_reconciliation_mismatches_total');logEvent('error',{event:'wallet.reconciliation_mismatch',correlationId:id,messageId:null,transactionId:null,walletId:id,providerId:null,checkedEntries:l.count});}
      return { walletId:id, storedBalance:{amount:w.balance,currency:w.currency.trim()}, calculatedBalance:{amount:calculated,currency:w.currency.trim()}, difference:{amount:difference,currency:w.currency.trim()}, consistent, checkedEntries:l.count };
    });
  }
  private async enqueue(q:any,event:IntegrationEvent<unknown>){await q('insert into outbox_messages(aggregate_id,event_type,payload) values($1,$2,$3::jsonb)',[event.aggregateId,event.eventType,JSON.stringify(event.toJSON())]);}
}
