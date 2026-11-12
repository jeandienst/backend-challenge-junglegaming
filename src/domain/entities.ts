import { Money } from './money';
import { FailureCode } from './failures';

function freezeDeep<T>(value:T):Readonly<T>{if(value&&typeof value==='object'){Object.freeze(value);for(const nested of Object.values(value as object))freezeDeep(nested);}return value;}

export interface WalletState { id:string; playerId:string; currency:string; balance:Money; version:number; createdAt:Date; updatedAt:Date }
export class Wallet {
  private constructor(readonly id:string,readonly playerId:string,readonly currency:string,private _balance:Money,private _version:number,readonly createdAt:Date,private _updatedAt:Date){}
  static open(p:{id:string;playerId:string;initialBalance:Money;now?:Date}){if(p.initialBalance.isNegative())throw new Error('NEGATIVE_OPENING_BALANCE');const now=new Date((p.now??new Date()).getTime());return new Wallet(p.id,p.playerId,p.initialBalance.currency,p.initialBalance,1,now,now);}
  static rehydrate(s:WalletState){return new Wallet(s.id,s.playerId,s.currency,s.balance,s.version,new Date(s.createdAt.getTime()),new Date(s.updatedAt.getTime()));}
  get balance(){return this._balance;} get version(){return this._version;} get updatedAt(){return this._updatedAt;}
  debit(money:Money,now=new Date()){this.assertMovement(money);const next=this._balance.subtract(money);if(next.isNegative())throw new Error('INSUFFICIENT_FUNDS');this.apply(next,now);return next;}
  credit(money:Money,now=new Date()){this.assertMovement(money);const next=this._balance.add(money);this.apply(next,now);return next;}
  private apply(next:Money,now:Date){if(!next.equals(this._balance)){this._balance=next;this._version++;this._updatedAt=new Date(now.getTime());}}
  private assertCurrency(m:Money){if(m.currency!==this.currency)throw new Error('CURRENCY_MISMATCH');}
  private assertMovement(m:Money){this.assertCurrency(m);if(!m.isPositive())throw new Error('INVALID_MOVEMENT');}
}

export enum WagerTransactionKind { Opening='OPENING',Bet='BET',Win='WIN',Loss='LOSS',Refund='REFUND',Rollback='ROLLBACK' }
export enum WagerTransactionStatus { Pending='PENDING',PendingReference='PENDING_REFERENCE',Processed='PROCESSED',Rejected='REJECTED',Failed='FAILED' }
export enum LedgerDirection { Debit='DEBIT',Credit='CREDIT' }
export interface WagerState { id:string;providerId:string;externalTransactionId:string;idempotencyKey:string;payloadHash:string;walletId:string;playerId:string;roundId:string;gameId:string;kind:WagerTransactionKind;money:Money;referenceExternalTransactionId?:string;referenceTransactionId?:string;createdAt:Date;status:WagerTransactionStatus;failureCode?:FailureCode;processedAt?:Date }
export class WagerTransaction {
  private constructor(private readonly state:WagerState){}
  static create(s:Omit<WagerState,'status'|'createdAt'> & {createdAt?:Date}){if((s.kind===WagerTransactionKind.Refund||s.kind===WagerTransactionKind.Rollback)&&!s.referenceExternalTransactionId)throw new Error('REFERENCE_REQUIRED');if(!s.money.isPositive())throw new Error('INVALID_MONEY');return new WagerTransaction({...s,status:WagerTransactionStatus.Pending,createdAt:new Date((s.createdAt??new Date()).getTime())});}
  static rehydrate(s:WagerState){return new WagerTransaction({...s,createdAt:new Date(s.createdAt.getTime()),processedAt:s.processedAt?new Date(s.processedAt.getTime()):undefined});}
  get status(){return this.state.status;} isTerminal(){return [WagerTransactionStatus.Processed,WagerTransactionStatus.Rejected,WagerTransactionStatus.Failed].includes(this.state.status);}
  affectsBalance(){return this.state.kind!==WagerTransactionKind.Loss;}
  requiresReference(){return this.state.kind===WagerTransactionKind.Refund||this.state.kind===WagerTransactionKind.Rollback;}
  ledgerDirectionFor(reference?:WagerTransaction){
    if(this.state.kind===WagerTransactionKind.Opening||this.state.kind===WagerTransactionKind.Win||this.state.kind===WagerTransactionKind.Refund)return LedgerDirection.Credit;
    if(this.state.kind===WagerTransactionKind.Bet)return LedgerDirection.Debit;
    if(this.state.kind===WagerTransactionKind.Loss)return undefined;
    if(!reference)throw new Error('REFERENCE_REQUIRED');
    if(reference.state.kind===WagerTransactionKind.Bet)return LedgerDirection.Credit;
    if([WagerTransactionKind.Win,WagerTransactionKind.Refund].includes(reference.state.kind))return LedgerDirection.Debit;
    throw new Error('INVALID_REFERENCE_KIND');
  }
  markPendingReference(){this.assertMutable();this.state.status=WagerTransactionStatus.PendingReference;}
  markProcessed(referenceId?:string,at=new Date()){this.assertMutable();this.state.status=WagerTransactionStatus.Processed;this.state.referenceTransactionId=referenceId;this.state.processedAt=new Date(at.getTime());}
  reject(code:FailureCode,at=new Date()){this.assertMutable();this.state.status=WagerTransactionStatus.Rejected;this.state.failureCode=code;this.state.processedAt=new Date(at.getTime());}
  fail(code:FailureCode,at=new Date()){this.assertMutable();this.state.status=WagerTransactionStatus.Failed;this.state.failureCode=code;this.state.processedAt=new Date(at.getTime());}
  matchesPayload(hash:string){return this.state.payloadHash===hash;}
  private assertMutable(){if(this.isTerminal())throw new Error('INVALID_TRANSACTION_STATE');}
}

export interface LedgerProps { id:string;walletId:string;transactionId:string;direction:LedgerDirection;money:Money;balanceBefore:Money;balanceAfter:Money;createdAt?:Date }
export class WalletLedgerEntry {
  private constructor(readonly id:string,readonly walletId:string,readonly transactionId:string,readonly direction:LedgerDirection,readonly money:Money,readonly balanceBefore:Money,readonly balanceAfter:Money,readonly createdAt:Date){}
  static create(p:LedgerProps){const computed=p.direction===LedgerDirection.Credit?p.balanceBefore.add(p.money):p.balanceBefore.subtract(p.money);if(!computed.equals(p.balanceAfter)||p.balanceAfter.isNegative())throw new Error('UNBALANCED_LEDGER_ENTRY');return new WalletLedgerEntry(p.id,p.walletId,p.transactionId,p.direction,p.money,p.balanceBefore,p.balanceAfter,new Date((p.createdAt??new Date()).getTime()));}
  static rehydrate(p:LedgerProps){return new WalletLedgerEntry(p.id,p.walletId,p.transactionId,p.direction,p.money,p.balanceBefore,p.balanceAfter,new Date((p.createdAt??new Date()).getTime()));}
  isBalanced(){try{const computed=this.direction===LedgerDirection.Credit?this.balanceBefore.add(this.money):this.balanceBefore.subtract(this.money);return computed.equals(this.balanceAfter);}catch{return false;}}
}

export class InboxMessage {
  private constructor(readonly messageId:string,readonly consumerName:string,readonly payloadHash:string,readonly receivedAt:Date,private _processedAt?:Date){}
  static receive(p:{messageId:string;consumerName:string;payloadHash:string;receivedAt?:Date}){return new InboxMessage(p.messageId,p.consumerName,p.payloadHash,new Date((p.receivedAt??new Date()).getTime()));}
  static rehydrate(p:{messageId:string;consumerName:string;payloadHash:string;receivedAt:Date;processedAt?:Date}){return new InboxMessage(p.messageId,p.consumerName,p.payloadHash,new Date(p.receivedAt.getTime()),p.processedAt?new Date(p.processedAt.getTime()):undefined);}
  get processedAt(){return this._processedAt;} isProcessed(){return !!this._processedAt;} markProcessed(at=new Date()){if(this._processedAt)throw new Error('INVALID_INBOX_STATE');this._processedAt=at;}
}

export class OutboxMessage {
  private constructor(readonly id:string,readonly aggregateId:string,readonly eventType:string,readonly payload:Readonly<Record<string,unknown>>,readonly occurredAt:Date,private _attempts:number,private _nextAttemptAt?:Date,private _publishedAt?:Date){}
  static enqueue(e:{id:string;aggregateId:string;eventType:string;payload:Record<string,unknown>;occurredAt?:Date}){return new OutboxMessage(e.id,e.aggregateId,e.eventType,freezeDeep(structuredClone(e.payload)),new Date((e.occurredAt??new Date()).getTime()),0);}
  static rehydrate(s:{id:string;aggregateId:string;eventType:string;payload:Readonly<Record<string,unknown>>;occurredAt:Date;attempts:number;nextAttemptAt?:Date;publishedAt?:Date}){return new OutboxMessage(s.id,s.aggregateId,s.eventType,freezeDeep(structuredClone(s.payload)),new Date(s.occurredAt.getTime()),s.attempts,s.nextAttemptAt?new Date(s.nextAttemptAt.getTime()):undefined,s.publishedAt?new Date(s.publishedAt.getTime()):undefined);}
  get attempts(){return this._attempts;}get nextAttemptAt(){return this._nextAttemptAt;}get publishedAt(){return this._publishedAt;}
  isPending(){return !this._publishedAt;}isDue(now:Date){return this.isPending()&&(!this._nextAttemptAt||this._nextAttemptAt<=now);}
  markPublished(at=new Date()){if(!this.isPending())throw new Error('INVALID_OUTBOX_STATE');this._publishedAt=at;}
  scheduleRetry(now=new Date()){if(!this.isPending())throw new Error('INVALID_OUTBOX_STATE');this._attempts++;this._nextAttemptAt=new Date(now.getTime()+Math.min(1000*2**Math.min(this._attempts,10),900000));}
}
