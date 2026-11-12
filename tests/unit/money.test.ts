import { describe, expect, test } from 'bun:test';
import { Money } from '../../src/domain/money';
import { Wallet, WalletLedgerEntry, LedgerDirection, WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from '../../src/domain/entities';

describe('Money',()=>{
  test('uses exact cents and fixed two decimal serialization',()=>{
    expect(Money.from({amount:'0.10',currency:'BRL'}).add(Money.from({amount:'0.20',currency:'BRL'})).toJSON()).toEqual({amount:'0.30',currency:'BRL'});
    expect(Money.from({amount:'999999999999999999.99',currency:'BRL'}).toJSON().amount).toBe('999999999999999999.99');
  });
  test.each(['','1','1.0','1.000','1e2','NaN','Infinity','-1.00','01.00'])('rejects invalid input %s',amount=>{
    expect(()=>Money.from({amount,currency:'BRL'})).toThrow();
  });
  test('rejects a three-letter string that is not an ISO currency',()=>expect(()=>Money.from({amount:'1.00',currency:'ZZZ'})).toThrow('INVALID_MONEY'));
  test('rejects arithmetic across currencies',()=>expect(()=>Money.from({amount:'1.00',currency:'BRL'}).add(Money.from({amount:'1.00',currency:'USD'}))).toThrow('CURRENCY_MISMATCH'));
});

describe('domain invariants',()=>{
  test('wallet debit and credit update version only when balance changes',()=>{
    const w=Wallet.open({id:'w',playerId:'p',initialBalance:Money.from({amount:'10.00',currency:'BRL'})});
    w.debit(Money.from({amount:'2.00',currency:'BRL'})); expect(w.balance.toJSON().amount).toBe('8.00');expect(w.version).toBe(2);
    expect(()=>w.debit(Money.from({amount:'9.00',currency:'BRL'}))).toThrow('INSUFFICIENT_FUNDS');
    expect(()=>w.credit(Money.from({amount:'1.00',currency:'USD'}))).toThrow('CURRENCY_MISMATCH');
  });
  test('ledger factory checks before/after arithmetic',()=>{
    const money=Money.from({amount:'2.00',currency:'BRL'});
    expect(()=>WalletLedgerEntry.create({id:'l',walletId:'w',transactionId:'t',direction:LedgerDirection.Debit,money,balanceBefore:Money.from({amount:'5.00',currency:'BRL'}),balanceAfter:Money.from({amount:'4.00',currency:'BRL'})})).toThrow('UNBALANCED_LEDGER_ENTRY');
  });
  test('transaction references and terminal state transitions are enforced',()=>{
    expect(()=>WagerTransaction.create({id:'t',providerId:'p',externalTransactionId:'e',idempotencyKey:'k',payloadHash:'h',walletId:'w',playerId:'p',roundId:'r',gameId:'g',kind:WagerTransactionKind.Refund,money:Money.from({amount:'1.00',currency:'BRL'})})).toThrow('REFERENCE_REQUIRED');
    const tx=WagerTransaction.rehydrate({id:'t',providerId:'p',externalTransactionId:'e',idempotencyKey:'k',payloadHash:'h',walletId:'w',playerId:'p',roundId:'r',gameId:'g',kind:WagerTransactionKind.Bet,money:Money.from({amount:'1.00',currency:'BRL'}),createdAt:new Date(),status:WagerTransactionStatus.Pending});
    tx.markProcessed();expect(()=>tx.reject('INSUFFICIENT_FUNDS')).toThrow('INVALID_TRANSACTION_STATE');
  });

  test('wager kinds map to the required ledger direction',()=>{
    const money=Money.from({amount:'1.00',currency:'BRL'});
    const create=(kind:WagerTransactionKind,referenceExternalTransactionId?:string)=>WagerTransaction.create({id:crypto.randomUUID(),providerId:'p',externalTransactionId:crypto.randomUUID(),idempotencyKey:crypto.randomUUID(),payloadHash:'a'.repeat(64),walletId:'w',playerId:'p',roundId:'r',gameId:'g',kind,money,referenceExternalTransactionId});
    const bet=create(WagerTransactionKind.Bet);
    const win=create(WagerTransactionKind.Win);
    const refund=create(WagerTransactionKind.Refund,'bet');
    const loss=create(WagerTransactionKind.Loss);
    const rollbackBet=create(WagerTransactionKind.Rollback,'bet');
    const rollbackWin=create(WagerTransactionKind.Rollback,'win');
    const processed=(transaction:WagerTransaction)=>WagerTransaction.rehydrate({id:'ref',providerId:'p',externalTransactionId:'ref',idempotencyKey:'ref',payloadHash:'a'.repeat(64),walletId:'w',playerId:'p',roundId:'r',gameId:'g',kind:transaction===bet?WagerTransactionKind.Bet:WagerTransactionKind.Win,money,createdAt:new Date(),status:WagerTransactionStatus.Processed});
    expect(bet.ledgerDirectionFor()).toBe(LedgerDirection.Debit);
    expect(win.ledgerDirectionFor()).toBe(LedgerDirection.Credit);
    expect(refund.ledgerDirectionFor()).toBe(LedgerDirection.Credit);
    expect(loss.ledgerDirectionFor()).toBeUndefined();
    expect(rollbackBet.ledgerDirectionFor(processed(bet))).toBe(LedgerDirection.Credit);
    expect(rollbackWin.ledgerDirectionFor(processed(win))).toBe(LedgerDirection.Debit);
    expect(()=>rollbackBet.ledgerDirectionFor()).toThrow('REFERENCE_REQUIRED');
  });
});
