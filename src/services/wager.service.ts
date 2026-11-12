import { ConflictException, Injectable, UnprocessableEntityException } from '@nestjs/common';
import { createHash, randomUUID } from 'node:crypto';
import { z } from 'zod';
import { Database } from '../infra/database';
import { Money } from '../domain/money';
import { Failure } from '../domain/failures';
import { IntegrationEvent, WagerTransactionFailed, WagerTransactionPendingReference, WagerTransactionProcessed, WagerTransactionRejected, WagerTransactionRequested, WalletBalanceChanged } from '../domain/events';
import { Metrics } from '../infra/metrics';
import { LedgerDirection, Wallet, WalletLedgerEntry, WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from '../domain/entities';
import { logEvent } from '../infra/logging';

const inputSchema = z.object({
  providerId: z.string().min(1).max(100), externalTransactionId: z.string().min(1).max(200),
  playerId: z.string().uuid(), walletId: z.string().uuid(), roundId: z.string().min(1).max(200),
  gameId: z.string().min(1).max(200), kind: z.enum(['BET','WIN','LOSS','REFUND','ROLLBACK']),
  money: z.object({ amount: z.string(), currency: z.string() }),
  referenceExternalTransactionId: z.string().min(1).max(200).optional(),
});
type Input = z.infer<typeof inputSchema>;
function rehydrateTransaction(row:any):WagerTransaction {
  return WagerTransaction.rehydrate({id:row.id,providerId:row.provider_id,externalTransactionId:row.external_transaction_id,idempotencyKey:row.idempotency_key,payloadHash:row.payload_hash,walletId:row.wallet_id,playerId:row.player_id,roundId:row.round_id,gameId:row.game_id,kind:row.kind as WagerTransactionKind,money:Money.from({amount:row.amount,currency:row.currency.trim()}),referenceExternalTransactionId:row.reference_external_transaction_id??undefined,referenceTransactionId:row.reference_transaction_id??undefined,createdAt:new Date(row.created_at),status:row.status as WagerTransactionStatus,failureCode:row.failure_code??undefined,processedAt:row.processed_at?new Date(row.processed_at):undefined});
}
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value && typeof value === 'object') return `{${Object.entries(value).sort(([a],[b])=>a.localeCompare(b)).map(([k,v])=>`${JSON.stringify(k)}:${canonical(v)}`).join(',')}}`;
  return JSON.stringify(value) ?? 'null';
}
function hash(input: Input): string { return createHash('sha256').update(canonical(input)).digest('hex'); }

@Injectable()
export class WagerService {
  constructor(private readonly db: Database, private readonly metrics:Metrics) {}
  async submit(raw: unknown, idempotencyKey: string) {
    const parsed=inputSchema.safeParse(raw);
    if (!parsed.success) throw new UnprocessableEntityException({ code:'INVALID_PAYLOAD', details:parsed.error.flatten() });
    const p=parsed.data;
    if (!idempotencyKey || idempotencyKey.length>255) throw new UnprocessableEntityException({code:'INVALID_IDEMPOTENCY_KEY'});
    let inputMoney:Money;
    try { inputMoney=Money.from(p.money); } catch { throw new UnprocessableEntityException({code:'INVALID_MONEY'}); }
    if(!inputMoney.isPositive()) throw new UnprocessableEntityException({code:'INVALID_MONEY'});
    if (p.kind==='REFUND'||p.kind==='ROLLBACK') { if(!p.referenceExternalTransactionId) throw new UnprocessableEntityException({code:'REFERENCE_REQUIRED'}); }
    try {
      return await this.db.tx(async q => {
        const digest=hash(p); const id=randomUUID();
        const transaction=WagerTransaction.create({id,providerId:p.providerId,externalTransactionId:p.externalTransactionId,idempotencyKey,payloadHash:digest,walletId:p.walletId,playerId:p.playerId,roundId:p.roundId,gameId:p.gameId,kind:p.kind as WagerTransactionKind,money:inputMoney,referenceExternalTransactionId:p.referenceExternalTransactionId});
        const inserted=await q<any>(`insert into wager_transactions(id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,currency,round_id,game_id,kind,amount,reference_external_transaction_id,status) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) on conflict do nothing returning id`, [id,p.providerId,p.externalTransactionId,idempotencyKey,digest,p.walletId,p.playerId,inputMoney.currency,p.roundId,p.gameId,p.kind,inputMoney.toJSON().amount,p.referenceExternalTransactionId??null,transaction.status]);
        if(!inserted.length) {
          const [old]=await q<any>('select * from wager_transactions where (provider_id=$1 and idempotency_key=$2) or (provider_id=$1 and external_transaction_id=$3) order by created_at limit 1',[p.providerId,idempotencyKey,p.externalTransactionId]);
          if(!old) throw new ConflictException({code:'IDEMPOTENCY_CONFLICT'});
          if(old.payload_hash!==digest || old.idempotency_key!==idempotencyKey) throw new ConflictException({code:'IDEMPOTENCY_CONFLICT'});
          this.metrics.inc('wager_idempotency_duplicates_total');
          return {
            transactionId:old.id,
            status:old.status,
            balance:old.balance_snapshot==null?null:{amount:String(old.balance_snapshot),currency:old.currency.trim()},
            ...(old.failure_code?{failureCode:old.failure_code}:{}),
            idempotentReplay:true,
          };
        }
        await this.event(q,p.walletId,WagerTransactionRequested.create({eventId:randomUUID(),aggregateId:p.walletId,correlationId:id,causationId:id,occurredAt:new Date(),data:{transactionId:id,providerId:p.providerId,externalTransactionId:p.externalTransactionId,idempotencyKey,walletId:p.walletId,playerId:p.playerId,roundId:p.roundId,gameId:p.gameId,kind:p.kind,money:inputMoney.toJSON(),...(p.referenceExternalTransactionId?{referenceExternalTransactionId:p.referenceExternalTransactionId}:{})}}));
        this.metrics.inc('wager_transactions_total',{status:'pending'});
        return {transactionId:id,status:'PENDING',balance:null,idempotentReplay:false};
      });
    } catch(e:any) { throw e; }
  }

  async process(transactionId: string, inbox?: { messageId:string; payloadHash:string }): Promise<void> {
    const started=Date.now();
    try { await this.db.tx(async q => {
      if (inbox) {
        const received=await q<any>(`insert into inbox_messages(consumer_name,message_id,payload_hash) values('wager-processor',$1,$2) on conflict do nothing returning message_id`,[inbox.messageId,inbox.payloadHash]);
        if (!received.length) { this.metrics.inc('wager_messages_duplicates_total'); return; }
        await q(`update inbox_messages set processed_at=now() where consumer_name='wager-processor' and message_id=$1`,[inbox.messageId]);
      }
      const transactionLockStarted=Date.now();
      const [tx]=await q<any>('select * from wager_transactions where id=$1 for update',[transactionId]);
      const transactionLockWait=Date.now()-transactionLockStarted;this.metrics.observe('wager_transaction_lock_wait_ms',transactionLockWait);if(transactionLockWait>5)this.metrics.inc('wager_lock_conflicts_total',{scope:'transaction'});
      if(!tx) throw new Error('TRANSACTION_NOT_FOUND');
      const wager=rehydrateTransaction(tx);
      if(wager.isTerminal()) return;
      if(!inbox&&tx.status==='PENDING_REFERENCE'&&tx.next_attempt_at&&new Date(tx.next_attempt_at)>new Date())return;
      const walletLockStarted=Date.now();
      const [wallet]=await q<any>('select * from wallets where id=$1 for update',[tx.wallet_id]);
      const walletLockWait=Date.now()-walletLockStarted;this.metrics.observe('wager_wallet_lock_wait_ms',walletLockWait);if(walletLockWait>5)this.metrics.inc('wager_lock_conflicts_total',{scope:'wallet'});
      if(!wallet) return this.reject(q,tx,Failure.WALLET_NOT_FOUND);
      if(wallet.player_id!==tx.player_id) return this.reject(q,tx,Failure.PLAYER_WALLET_MISMATCH);
      if(wallet.currency.trim()!==tx.currency.trim()) return this.reject(q,tx,Failure.CURRENCY_MISMATCH);

      let reference:any=null;
      const hasReference=tx.kind==='REFUND'||tx.kind==='ROLLBACK'||(tx.kind==='WIN'&&tx.reference_external_transaction_id);
      if(hasReference) {
        [reference]=await q<any>('select * from wager_transactions where provider_id=$1 and external_transaction_id=$2 for update',[tx.provider_id,tx.reference_external_transaction_id]);
        if(!reference) return this.deferReference(q,tx,wager);
        if(reference.player_id!==tx.player_id||reference.wallet_id!==tx.wallet_id||reference.currency!==tx.currency||reference.round_id!==tx.round_id||reference.provider_id!==tx.provider_id) return this.reject(q,tx,Failure.REFERENCE_CONTEXT_MISMATCH);
        if(reference.status==='PENDING'||reference.status==='PENDING_REFERENCE') return this.deferReference(q,tx,wager);
        if(reference.status!=='PROCESSED') return this.reject(q,tx,Failure.INVALID_REFERENCE_KIND);
        const allowed=tx.kind==='REFUND'?reference.kind==='BET':tx.kind==='ROLLBACK'?['BET','WIN','REFUND'].includes(reference.kind):reference.kind==='BET';
        if(!allowed) return this.reject(q,tx,Failure.INVALID_REFERENCE_KIND);
        if((tx.kind==='REFUND'||tx.kind==='ROLLBACK')&&Money.from({amount:reference.amount,currency:reference.currency.trim()}).equals(Money.from({amount:tx.amount,currency:tx.currency.trim()}))===false) return this.reject(q,tx,Failure.REFERENCE_AMOUNT_MISMATCH);
        if(tx.kind==='REFUND'||tx.kind==='ROLLBACK') {
          const [prior]=await q<any>('select id from wager_transactions where reference_transaction_id=$1 and kind=$2 and status in (\'PROCESSED\',\'PENDING\',\'PENDING_REFERENCE\') and id<>$3',[reference.id,tx.kind,tx.id]);
          if(prior) return this.reject(q,tx,Failure.ALREADY_REVERSED);
        }
      }

      const before=Money.from({amount:wallet.balance,currency:wallet.currency.trim()});
      const amount=Money.from({amount:tx.amount,currency:tx.currency.trim()});
      const aggregate=Wallet.rehydrate({id:wallet.id,playerId:wallet.player_id,currency:wallet.currency.trim(),balance:before,version:wallet.version,createdAt:new Date(wallet.created_at),updatedAt:new Date(wallet.updated_at)});
      const referenceAggregate=reference?rehydrateTransaction(reference):undefined;
      const domainDirection=wager.ledgerDirectionFor(referenceAggregate);
      const direction:LedgerDirection|undefined=domainDirection;
      if(direction===LedgerDirection.Debit&&before.isLessThan(amount)) return this.reject(q,tx,tx.kind==='ROLLBACK'?Failure.REVERSAL_WOULD_OVERDRAW:Failure.INSUFFICIENT_FUNDS);
      const after=direction===LedgerDirection.Credit?aggregate.credit(amount):direction===LedgerDirection.Debit?aggregate.debit(amount):before;
      if(direction) {
        const entry=WalletLedgerEntry.create({id:randomUUID(),walletId:wallet.id,transactionId:tx.id,direction,money:amount,balanceBefore:before,balanceAfter:after});
        await q(`update wallets set balance=$2,version=$3,updated_at=$4 where id=$1`,[wallet.id,after.toJSON().amount,aggregate.version,aggregate.updatedAt]);
        await q(`insert into wallet_ledger(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after,created_at) values($1,$2,$3,$4,$5,$6,$7,$8,$9)`,[entry.id,entry.walletId,entry.transactionId,entry.direction,entry.money.toJSON().amount,wallet.currency.trim(),entry.balanceBefore.toJSON().amount,entry.balanceAfter.toJSON().amount,entry.createdAt]);
        await this.event(q,wallet.id,WalletBalanceChanged.create({eventId:randomUUID(),aggregateId:wallet.id,correlationId:tx.id,causationId:tx.id,occurredAt:new Date(),data:{walletId:wallet.id,transactionId:tx.id,direction,money:amount.toJSON(),balanceBefore:before.toJSON(),balanceAfter:after.toJSON(),walletVersion:wallet.version+1}}));
      }
      wager.markProcessed(reference?.id, new Date());
      await q(`update wager_transactions set status='PROCESSED',reference_transaction_id=$2,processed_at=now(),balance_snapshot=$3,next_attempt_at=null where id=$1`,[tx.id,reference?.id??null,after.toJSON().amount]);
      await this.event(q,wallet.id,WagerTransactionProcessed.create({eventId:randomUUID(),aggregateId:wallet.id,correlationId:tx.id,causationId:tx.id,occurredAt:new Date(),data:{transactionId:tx.id,kind:tx.kind,balance:after.toJSON()}}));
      this.metrics.inc('wager_transactions_total',{status:'processed'});
    }); } catch(e:any) { if(e?.code==='40P01'||e?.code==='55P03') this.metrics.inc('wager_lock_conflicts_total'); throw e; }
    finally { this.metrics.observe('wager_processing_duration_ms',Date.now()-started); }
    const [summary]=await this.db.query<any>('select id,provider_id,wallet_id,status from wager_transactions where id=$1',[transactionId]);
    if(summary)logEvent('info',{event:'wager.processed',correlationId:transactionId,messageId:inbox?.messageId??null,transactionId:summary.id,walletId:summary.wallet_id,providerId:summary.provider_id,status:summary.status});
  }
  private async reject(q:any,tx:any,code:typeof Failure[keyof typeof Failure]) {
    rehydrateTransaction(tx).reject(code);
    const [w]=await q('select balance,currency from wallets where id=$1',[tx.wallet_id]);
    await q(`update wager_transactions set status='REJECTED',failure_code=$2,processed_at=now(),balance_snapshot=$3 where id=$1`,[tx.id,code,w?.balance??null]);
    await this.event(q,tx.wallet_id,WagerTransactionRejected.create({eventId:randomUUID(),aggregateId:tx.wallet_id,correlationId:tx.id,causationId:tx.id,occurredAt:new Date(),data:{transactionId:tx.id,failureCode:code}}));
    this.metrics.inc('wager_transactions_total',{status:'rejected',failure_code:code});
  }
  private async deferReference(q:any,tx:any,wager:WagerTransaction) {
    const attempts=Number(tx.attempts)+1;
    if(attempts>=12)return this.reject(q,tx,Failure.REFERENCE_NOT_FOUND);
    const delay=Math.min(30_000*2**Math.min(attempts-1,6),1_800_000);
    wager.markPendingReference();
    await q(`update wager_transactions set status='PENDING_REFERENCE',attempts=$2,next_attempt_at=now()+($3::text||' milliseconds')::interval where id=$1`,[tx.id,attempts,delay]);
    await this.event(q,tx.wallet_id,WagerTransactionPendingReference.create({eventId:randomUUID(),aggregateId:tx.wallet_id,correlationId:tx.id,causationId:tx.id,occurredAt:new Date(),data:{transactionId:tx.id,referenceExternalTransactionId:tx.reference_external_transaction_id,attempt:attempts}}));
    this.metrics.inc('wager_retries_total',{reason:'pending_reference'});
    this.metrics.inc('wager_transactions_total',{status:'pending_reference'});
  }
  async fail(transactionId:string,inbox:{messageId:string;payloadHash:string}){
    await this.db.tx(async q=>{
      const received=await q<any>(`insert into inbox_messages(consumer_name,message_id,payload_hash) values('wager-dlq',$1,$2) on conflict do nothing returning message_id`,[inbox.messageId,inbox.payloadHash]);
      if(!received.length)return;
      await q(`update inbox_messages set processed_at=now() where consumer_name='wager-dlq' and message_id=$1`,[inbox.messageId]);
      const [tx]=await q<any>('select * from wager_transactions where id=$1 for update',[transactionId]);
      if(!tx)throw new Error('TRANSACTION_NOT_FOUND');
      const wager=rehydrateTransaction(tx);if(wager.isTerminal())return;
      wager.fail(Failure.MAX_RETRIES_EXCEEDED);
      await q(`update wager_transactions set status='FAILED',failure_code=$2,processed_at=now() where id=$1`,[transactionId,Failure.MAX_RETRIES_EXCEEDED]);
      await this.event(q,tx.wallet_id,WagerTransactionFailed.create({eventId:randomUUID(),aggregateId:tx.wallet_id,correlationId:tx.id,causationId:inbox.messageId,occurredAt:new Date(),data:{transactionId:tx.id,failureCode:Failure.MAX_RETRIES_EXCEEDED}}));
      this.metrics.inc('wager_transactions_total',{status:'failed',failure_code:Failure.MAX_RETRIES_EXCEEDED});
    });
  }
  private async event(q:any,aggregateId:string,event:IntegrationEvent<unknown>) {
    await q('insert into outbox_messages(aggregate_id,event_type,payload) values($1,$2,$3::jsonb)',[aggregateId,event.eventType,JSON.stringify(event.toJSON())]);
  }
  async get(id:string) { const [r]=await this.db.query<any>('select * from wager_transactions where id=$1',[id]); return this.view(r); }
  async getByExternal(providerId:string,externalId:string) { const [r]=await this.db.query<any>('select * from wager_transactions where provider_id=$1 and external_transaction_id=$2',[providerId,externalId]); return this.view(r); }
  async verifyQueuedRequest(transactionId:string,raw:unknown){
    const parsed=inputSchema.safeParse(raw);if(!parsed.success)throw new Error('INVALID_QUEUE_PAYLOAD');
    const [stored]=await this.db.query<any>('select payload_hash,idempotency_key from wager_transactions where id=$1',[transactionId]);
    if(!stored)throw new Error('TRANSACTION_NOT_FOUND');
    if(hash(parsed.data)!==stored.payload_hash||parsed.data.providerId===undefined)throw new Error('QUEUE_PAYLOAD_MISMATCH');
    const key=(raw as {idempotencyKey?:unknown})?.idempotencyKey;if(key!==stored.idempotency_key)throw new Error('QUEUE_IDEMPOTENCY_MISMATCH');
  }
  private view(r:any) { if(!r)return null;return {transactionId:r.id,providerId:r.provider_id,externalTransactionId:r.external_transaction_id,walletId:r.wallet_id,playerId:r.player_id,roundId:r.round_id,gameId:r.game_id,kind:r.kind,money:{amount:r.amount,currency:r.currency.trim()},referenceExternalTransactionId:r.reference_external_transaction_id??null,referenceTransactionId:r.reference_transaction_id??null,status:r.status,failureCode:r.failure_code??null,balance:r.balance_snapshot?{amount:r.balance_snapshot,currency:r.currency.trim()}:null,createdAt:new Date(r.created_at).toISOString(),processedAt:r.processed_at?new Date(r.processed_at).toISOString():null}; }
  async retryPendingReferences() { const rows=await this.db.query<any>(`select id from wager_transactions where status='PENDING_REFERENCE' and next_attempt_at<=now() order by next_attempt_at limit 100`); for(const row of rows) await this.process(row.id); }
}
