import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { Database } from '../../src/infra/database';
import { CreateQueueCommand, DeleteQueueCommand, GetQueueAttributesCommand, ReceiveMessageCommand, SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { Metrics } from '../../src/infra/metrics';
import { WalletService } from '../../src/services/wallet.service';
import { WagerService } from '../../src/services/wager.service';
import { QueueWorkers } from '../../src/workers/queue.workers';

const enabled=Boolean(process.env.DATABASE_URL_TEST);
describe.skipIf(!enabled)('PostgreSQL transaction and concurrency guarantees',()=>{
  const db=new Database();const metrics=new Metrics();let wallets:WalletService;let wagers:WagerService;const players:string[]=[];
  beforeAll(async()=>{process.env.DATABASE_URL=process.env.DATABASE_URL_TEST;await db.onModuleInit();await db.orm.getMigrator().up();wallets=new WalletService(db,metrics);wagers=new WagerService(db,metrics);},30_000);
  afterAll(async()=>{
    if(players.length){await db.query('delete from outbox_messages where aggregate_id in(select id from wallets where player_id=any($1::uuid[]))',[players]);await db.query('alter table wallet_ledger disable trigger wallet_ledger_immutable');await db.query('alter table wager_transactions disable trigger wager_transaction_immutable_fields');await db.query('delete from wallet_ledger where wallet_id in(select id from wallets where player_id=any($1::uuid[]))',[players]);await db.query('delete from wager_transactions where player_id=any($1::uuid[])',[players]);await db.query('delete from wallets where player_id=any($1::uuid[])',[players]);await db.query('alter table wallet_ledger enable trigger wallet_ledger_immutable');await db.query('alter table wager_transactions enable trigger wager_transaction_immutable_fields');}
    await db.onModuleDestroy();
  },30_000);
  const makePlayer=()=>{const id=crypto.randomUUID();players.push(id);return id;};
  const request=(playerId:string,walletId:string,n:string,amount:string)=>({providerId:'integration',externalTransactionId:`${n}-${crypto.randomUUID()}`,playerId,walletId,roundId:'round',gameId:'test-game',kind:'BET',money:{amount,currency:'BRL'}});

  test('migrations enforce wallet uniqueness, non-negative balance and immutable ledger rows',async()=>{
    const playerId=makePlayer();const wallet=await wallets.create(playerId,{amount:'1.00',currency:'BRL'});
    await expect(wallets.create(playerId,{amount:'0.00',currency:'BRL'})).rejects.toMatchObject({status:409});
    await expect(db.query('update wallets set balance=$2 where id=$1',[wallet.id,'-1.00'])).rejects.toThrow();
    const [opening]=await db.query<any>('select id from wallet_ledger where wallet_id=$1',[wallet.id]);
    await expect(db.query('update wallet_ledger set amount=amount where id=$1',[opening.id])).rejects.toThrow();
  });

  test('ledger pagination uses a stable cursor and normalized money fields',async()=>{
    const playerId=makePlayer();const wallet=await wallets.create(playerId,{amount:'5.00',currency:'BRL'});
    const bet=await wagers.submit(request(playerId,wallet.id,'ledger-cursor','1.00'),`ledger-${playerId}`);await wagers.process(bet.transactionId);
    const first=await wallets.ledger(wallet.id,undefined,1);
    expect(first.items).toHaveLength(1);expect(typeof first.nextCursor).toBe('string');expect(first.items[0]?.money.currency).toBe('BRL');
    const second=await wallets.ledger(wallet.id,first.nextCursor!,1);
    expect(second.items).toHaveLength(1);expect(second.items[0]?.id).not.toBe(first.items[0]?.id);
    await expect(wallets.ledger(crypto.randomUUID())).rejects.toMatchObject({status:404});
  });

  test('wallet, ledger, inbox and outbox roll back atomically and can recover on redelivery',async()=>{
    const playerId=makePlayer();const wallet=await wallets.create(playerId,{amount:'10.00',currency:'BRL'});
    const body=request(playerId,wallet.id,'atomic','2.00');const submitted=await wagers.submit(body,`atomic-${playerId}`);
    await db.query(`create function fail_integration_balance_event() returns trigger language plpgsql as $$ begin if new.event_type='WalletBalanceChanged' then raise exception 'injected outbox failure'; end if; return new; end $$`);
    await db.query('create trigger fail_integration_balance_event before insert on outbox_messages for each row execute function fail_integration_balance_event()');
    try {
      await expect(wagers.process(submitted.transactionId,{messageId:`atomic-${submitted.transactionId}`,payloadHash:'b'.repeat(64)})).rejects.toThrow('injected outbox failure');
    } finally {
      await db.query('drop trigger fail_integration_balance_event on outbox_messages');
      await db.query('drop function fail_integration_balance_event()');
    }
    const [unchanged]=await db.query<any>('select balance from wallets where id=$1',[wallet.id]);
    const [txBefore]=await db.query<any>('select status from wager_transactions where id=$1',[submitted.transactionId]);
    const [ledgerBefore]=await db.query<any>('select count(*)::int as n from wallet_ledger where wallet_id=$1',[wallet.id]);
    const [inboxBefore]=await db.query<any>('select count(*)::int as n from inbox_messages where message_id=$1',[`atomic-${submitted.transactionId}`]);
    const [eventsBefore]=await db.query<any>(`select count(*)::int as n from outbox_messages where payload->'data'->>'transactionId'=$1 and event_type in ('WalletBalanceChanged','WagerTransactionProcessed')`,[submitted.transactionId]);
    expect(unchanged.balance).toBe('10.00');expect(txBefore.status).toBe('PENDING');expect(ledgerBefore.n).toBe(1);expect(inboxBefore.n).toBe(0);expect(eventsBefore.n).toBe(0);

    await wagers.process(submitted.transactionId,{messageId:`atomic-${submitted.transactionId}`,payloadHash:'b'.repeat(64)});
    const [recovered]=await db.query<any>('select balance from wallets where id=$1',[wallet.id]);
    const [inboxAfter]=await db.query<any>('select count(*)::int as n from inbox_messages where message_id=$1 and processed_at is not null',[`atomic-${submitted.transactionId}`]);
    const [ledgerAfter]=await db.query<any>('select count(*)::int as n from wallet_ledger where wallet_id=$1',[wallet.id]);
    expect(recovered.balance).toBe('8.00');expect(inboxAfter.n).toBe(1);expect(ledgerAfter.n).toBe(2);
  },30_000);

  test('50 simultaneous identical submissions and deliveries create one debit',async()=>{
    const playerId=makePlayer();const wallet=await wallets.create(playerId,{amount:'100.00',currency:'BRL'});const body=request(playerId,wallet.id,'duplicate','1.00');
    const responses=await Promise.all(Array.from({length:50},(_,i)=>wagers.submit(body,`same-key-${i===0?'one':'one'}`)));
    const ids=new Set(responses.map(r=>r.transactionId));expect(ids.size).toBe(1);const txId=responses[0]!.transactionId;
    const messageId=`sqs-${txId}`;const payloadHash='a'.repeat(64);
    await Promise.all(Array.from({length:50},()=>wagers.process(txId,{messageId,payloadHash})));
    // Model a worker that committed and died before DeleteMessage: a fresh process redelivers the same SQS envelope.
    const restarted=Bun.spawn([process.execPath,'run','tests/integration/process-wager.ts'],{env:{...process.env,DATABASE_URL:process.env.DATABASE_URL_TEST,WAGER_TRANSACTION_ID:txId,WAGER_MESSAGE_ID:messageId,WAGER_PAYLOAD_HASH:payloadHash},stdout:'pipe',stderr:'pipe'});
    expect(await restarted.exited).toBe(0);
    const [stored]=await db.query<any>('select balance from wallets where id=$1',[wallet.id]);const [count]=await db.query<any>(`select count(*)::int as n from wallet_ledger where wallet_id=$1 and direction='DEBIT'`,[wallet.id]);
    const [inbox]=await db.query<any>(`select count(*)::int as n from inbox_messages where message_id=$1 and processed_at is not null`,[`sqs-${txId}`]);
    const [events]=await db.query<any>(`select count(*) filter(where event_type='WagerTransactionRequested')::int as requested,count(*) filter(where event_type='WagerTransactionProcessed')::int as processed,count(*) filter(where event_type='WalletBalanceChanged')::int as changed from outbox_messages where aggregate_id=$1`,[wallet.id]);
    expect(stored.balance).toBe('99.00');expect(count.n).toBe(1);expect(inbox.n).toBe(1);expect(events).toEqual({requested:1,processed:2,changed:2});
    expect((await wallets.reconcile(wallet.id)).consistent).toBe(true);
  },30_000);

  test('two concurrent 80 BRL bets against 100 BRL produce one debit',async()=>{
    const playerId=makePlayer();const wallet=await wallets.create(playerId,{amount:'100.00',currency:'BRL'});
    const [a,b]=await Promise.all([wagers.submit(request(playerId,wallet.id,'race-a','80.00'),'race-key-a'),wagers.submit(request(playerId,wallet.id,'race-b','80.00'),'race-key-b')]);
    const childIds=[a.transactionId,b.transactionId,a.transactionId];
    const children=childIds.map(transactionId=>Bun.spawn([process.execPath,'run','tests/integration/process-wager.ts'],{env:{...process.env,DATABASE_URL:process.env.DATABASE_URL_TEST,WAGER_TRANSACTION_ID:transactionId},stdout:'pipe',stderr:'pipe'}));
    const exits=await Promise.all(children.map(child=>child.exited));
    expect(exits).toEqual([0,0,0]);
    const [stored]=await db.query<any>('select balance from wallets where id=$1',[wallet.id]);const statuses=await db.query<any>('select status from wager_transactions where id=any($1::uuid[])',[ [a.transactionId,b.transactionId] ]);
    const [count]=await db.query<any>(`select count(*)::int as n from wallet_ledger where wallet_id=$1 and direction='DEBIT'`,[wallet.id]);
    expect(stored.balance).toBe('20.00');expect(statuses.map((x:any)=>x.status).sort()).toEqual(['PROCESSED','REJECTED']);expect(count.n).toBe(1);
  },30_000);

  test('out-of-order rollback waits for its reference and reversal cannot be repeated',async()=>{
    const playerId=makePlayer();const wallet=await wallets.create(playerId,{amount:'10.00',currency:'BRL'});
    const rollbackBody={...request(playerId,wallet.id,'rollback','3.00'),kind:'ROLLBACK',referenceExternalTransactionId:'future-win'};
    const rollback=await wagers.submit(rollbackBody,'rollback-key');await wagers.process(rollback.transactionId);
    let [pending]=await db.query<any>('select status from wager_transactions where id=$1',[rollback.transactionId]);expect(pending.status).toBe('PENDING_REFERENCE');
    const winBody={...request(playerId,wallet.id,'future-win','3.00'),kind:'WIN',externalTransactionId:'future-win'};
    const win=await wagers.submit(winBody,'win-key');await wagers.process(win.transactionId);await db.query(`update wager_transactions set next_attempt_at=now() where id=$1`,[rollback.transactionId]);await wagers.retryPendingReferences();
    const duplicateBody={...request(playerId,wallet.id,'rollback-again','3.00'),kind:'ROLLBACK',referenceExternalTransactionId:'future-win'};
    const duplicate=await wagers.submit(duplicateBody,'rollback-key-2');await wagers.process(duplicate.transactionId);
    const [finalWallet]=await db.query<any>('select balance from wallets where id=$1',[wallet.id]);
    const [finalRollback]=await db.query<any>('select status,failure_code from wager_transactions where id=$1',[duplicate.transactionId]);
    expect(finalWallet.balance).toBe('10.00');expect(finalRollback.status).toBe('REJECTED');expect(finalRollback.failure_code).toBe('ALREADY_REVERSED');
  },30_000);

  test('refund waits for a reference row that exists but is not processed yet',async()=>{
    const playerId=makePlayer();const wallet=await wallets.create(playerId,{amount:'10.00',currency:'BRL'});
    const betBody=request(playerId,wallet.id,'pending-bet','2.00');const bet=await wagers.submit(betBody,`pending-bet-${playerId}`);
    const refundBody={...request(playerId,wallet.id,'pending-refund','2.00'),kind:'REFUND',referenceExternalTransactionId:betBody.externalTransactionId};
    const refund=await wagers.submit(refundBody,`pending-refund-${playerId}`);await wagers.process(refund.transactionId);
    const [waiting]=await db.query<any>('select status from wager_transactions where id=$1',[refund.transactionId]);expect(waiting.status).toBe('PENDING_REFERENCE');
    await wagers.process(bet.transactionId);await db.query('update wager_transactions set next_attempt_at=now() where id=$1',[refund.transactionId]);await wagers.retryPendingReferences();
    const [result]=await db.query<any>('select status from wager_transactions where id=$1',[refund.transactionId]);const [balance]=await db.query<any>('select balance from wallets where id=$1',[wallet.id]);
    expect(result.status).toBe('PROCESSED');expect(balance.balance).toBe('10.00');
  },30_000);

  test('WIN may reference a processed BET from the same round without matching its amount',async()=>{
    const playerId=makePlayer();const wallet=await wallets.create(playerId,{amount:'10.00',currency:'BRL'});
    const betBody=request(playerId,wallet.id,'win-reference-bet','2.00');const bet=await wagers.submit(betBody,`win-bet-${playerId}`);await wagers.process(bet.transactionId);
    const winBody={...request(playerId,wallet.id,'win-reference','3.00'),kind:'WIN',referenceExternalTransactionId:betBody.externalTransactionId};
    const win=await wagers.submit(winBody,`win-${playerId}`);await wagers.process(win.transactionId);
    const result=await wagers.get(win.transactionId);const [balance]=await db.query<any>('select balance from wallets where id=$1',[wallet.id]);
    expect(result?.referenceTransactionId).toBe(bet.transactionId);expect(result?.status).toBe('PROCESSED');expect(balance.balance).toBe('11.00');
  },30_000);

  test('BET, REFUND and LOSS preserve ledger and balance semantics',async()=>{
    const playerId=makePlayer();const wallet=await wallets.create(playerId,{amount:'10.00',currency:'BRL'});
    const betBody=request(playerId,wallet.id,'bet-for-refund','2.00');const bet=await wagers.submit(betBody,'bet-refund-key');await wagers.process(bet.transactionId);
    const refundBody={...request(playerId,wallet.id,'refund','2.00'),kind:'REFUND',referenceExternalTransactionId:betBody.externalTransactionId};
    const refund=await wagers.submit(refundBody,'refund-key');await wagers.process(refund.transactionId);
    const loss=await wagers.submit({...request(playerId,wallet.id,'loss','2.00'),kind:'LOSS'},'loss-key');await wagers.process(loss.transactionId);
    const duplicateRefund=await wagers.submit({...request(playerId,wallet.id,'refund-again','2.00'),kind:'REFUND',referenceExternalTransactionId:betBody.externalTransactionId},'refund-again-key');await wagers.process(duplicateRefund.transactionId);
    const [stored]=await db.query<any>('select balance from wallets where id=$1',[wallet.id]);const [entries]=await db.query<any>('select count(*)::int as n from wallet_ledger where wallet_id=$1',[wallet.id]);
    const [rejected]=await db.query<any>('select status,failure_code from wager_transactions where id=$1',[duplicateRefund.transactionId]);
    expect(stored.balance).toBe('10.00');expect(entries.n).toBe(3);expect(rejected).toEqual({status:'REJECTED',failure_code:'ALREADY_REVERSED'});
  },30_000);

  test('same idempotency key with a different business payload conflicts',async()=>{
    const playerId=makePlayer();const wallet=await wallets.create(playerId,{amount:'10.00',currency:'BRL'});const original=request(playerId,wallet.id,'conflict','1.00');
    await wagers.submit(original,'conflict-key');
    await expect(wagers.submit({...original,money:{amount:'2.00',currency:'BRL'}},'conflict-key')).rejects.toMatchObject({status:409});
  });

  test('identical replay after processing returns the original result and balance snapshot',async()=>{
    const playerId=makePlayer();const wallet=await wallets.create(playerId,{amount:'10.00',currency:'BRL'});
    const body=request(playerId,wallet.id,'stable-replay','1.00');const key=`stable-replay-${playerId}`;
    const accepted=await wagers.submit(body,key);await wagers.process(accepted.transactionId);
    const replay=await wagers.submit(body,key);
    expect(accepted).toEqual({transactionId:replay.transactionId,status:'PENDING',balance:null,idempotentReplay:false});
    expect(replay).toEqual({transactionId:accepted.transactionId,status:'PROCESSED',balance:{amount:'9.00',currency:'BRL'},idempotentReplay:true});
    expect((await wagers.get(accepted.transactionId))?.status).toBe('PROCESSED');
  });

  test('replay of a rejected wager returns its failure and observed balance',async()=>{
    const playerId=makePlayer();const wallet=await wallets.create(playerId,{amount:'1.00',currency:'BRL'});
    const body=request(playerId,wallet.id,'rejected-replay','2.00');const key=`rejected-replay-${playerId}`;
    const accepted=await wagers.submit(body,key);await wagers.process(accepted.transactionId);
    const replay=await wagers.submit(body,key);
    expect(replay).toEqual({transactionId:accepted.transactionId,status:'REJECTED',balance:{amount:'1.00',currency:'BRL'},failureCode:'INSUFFICIENT_FUNDS',idempotentReplay:true});
  });

  test('database rejects a ledger entry for an already processed LOSS',async()=>{
    const playerId=makePlayer();const wallet=await wallets.create(playerId,{amount:'5.00',currency:'BRL'});
    const loss=await wagers.submit({...request(playerId,wallet.id,'loss-ledger-guard','1.00'),kind:'LOSS'},`loss-ledger-guard-${playerId}`);
    await wagers.process(loss.transactionId);

    await expect(db.tx(async q=>{
      await q('update wallets set balance=$2,version=version+1,updated_at=now() where id=$1',[wallet.id,'6.00']);
      await q(`insert into wallet_ledger(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after)
        values($1,$2,$3,'CREDIT','1.00','BRL','5.00','6.00')`,[crypto.randomUUID(),wallet.id,loss.transactionId]);
    })).rejects.toThrow('LOSS cannot have a ledger entry');

    const [stored]=await db.query<any>('select balance from wallets where id=$1',[wallet.id]);
    const [entries]=await db.query<any>('select count(*)::int as n from wallet_ledger where wallet_id=$1',[wallet.id]);
    expect(stored.balance).toBe('5.00');expect(entries.n).toBe(1);
  },30_000);

  test('different wallets make progress in parallel',async()=>{
    const contexts=await Promise.all(Array.from({length:8},async()=>{const playerId=makePlayer();const wallet=await wallets.create(playerId,{amount:'100.00',currency:'BRL'});const submitted=await wagers.submit(request(playerId,wallet.id,'parallel','10.00'),`parallel-${playerId}`);return {wallet,submitted};}));
    await Promise.all(contexts.map(x=>wagers.process(x.submitted.transactionId)));
    const balances=await Promise.all(contexts.map(async x=>(await db.query<any>('select balance from wallets where id=$1',[x.wallet.id]))[0]!.balance));
    expect(balances).toEqual(Array(8).fill('90.00'));
  },30_000);

  test('two outbox publishers claim disjoint rows',async()=>{
    const playerId=makePlayer();const wallet=await wallets.create(playerId,{amount:'1.00',currency:'BRL'});
    const claim=`with due as (select id from outbox_messages where aggregate_id=$1 and published_at is null and (locked_until is null or locked_until<now()) order by occurred_at limit 20 for update skip locked) update outbox_messages o set locked_until=now()+interval '30 seconds' from due where o.id=due.id returning o.id`;
    const [first,second]=await Promise.all([db.query<any>(claim,[wallet.id]),db.query<any>(claim,[wallet.id])]);
    const ids=[...first,...second].map(r=>r.id);expect(new Set(ids).size).toBe(2);expect(first.length+second.length).toBe(2);
  },30_000);

  test('real queue worker commits and ACKs commands, then records DLQ failures',async()=>{
    if(!process.env.SQS_ENDPOINT)throw new Error('SQS_ENDPOINT is required for the worker integration test');
    const client=new SQSClient({region:process.env.AWS_REGION??'us-east-1',endpoint:process.env.SQS_ENDPOINT,credentials:{accessKeyId:process.env.AWS_ACCESS_KEY_ID??'test',secretAccessKey:process.env.AWS_SECRET_ACCESS_KEY??'test'}});
    const suffix=crypto.randomUUID();let commandUrl:string|undefined;let deadLetterUrl:string|undefined;let worker:QueueWorkers|undefined;
    const previousCommandUrl=process.env.SQS_QUEUE_URL;const previousDlqUrl=process.env.SQS_DLQ_QUEUE_URL;
    try {
      commandUrl=(await client.send(new CreateQueueCommand({QueueName:`worker-command-${suffix}.fifo`,Attributes:{FifoQueue:'true',ContentBasedDeduplication:'false'}}))).QueueUrl;
      deadLetterUrl=(await client.send(new CreateQueueCommand({QueueName:`worker-dlq-${suffix}.fifo`,Attributes:{FifoQueue:'true',ContentBasedDeduplication:'false'}}))).QueueUrl;
      if(!commandUrl||!deadLetterUrl)throw new Error('SQS did not return isolated queue URLs');
      process.env.SQS_QUEUE_URL=commandUrl;process.env.SQS_DLQ_QUEUE_URL=deadLetterUrl;
      worker=new QueueWorkers(db,wagers,metrics);

      const playerId=makePlayer();const wallet=await wallets.create(playerId,{amount:'5.00',currency:'BRL'});
      const body=request(playerId,wallet.id,'worker-command','1.00');const submitted=await wagers.submit(body,`worker-${playerId}`);
      const [requestEvent]=await db.query<any>(`select payload from outbox_messages where event_type='WagerTransactionRequested' and payload->'data'->>'transactionId'=$1`,[submitted.transactionId]);
      const messageBody=JSON.stringify({...requestEvent.payload,messageId:requestEvent.payload.eventId,type:'WagerTransactionRequested'});
      await client.send(new SendMessageCommand({QueueUrl:commandUrl,MessageBody:messageBody,MessageGroupId:wallet.id,MessageDeduplicationId:requestEvent.payload.eventId}));
      await (worker as unknown as {consumeBatch:()=>Promise<void>}).consumeBatch();
      const [processed]=await db.query<any>('select status from wager_transactions where id=$1',[submitted.transactionId]);
      const [stored]=await db.query<any>('select balance from wallets where id=$1',[wallet.id]);
      const remaining=await client.send(new ReceiveMessageCommand({QueueUrl:commandUrl,WaitTimeSeconds:1,VisibilityTimeout:0,MaxNumberOfMessages:1}));
      expect(processed.status).toBe('PROCESSED');expect(stored.balance).toBe('4.00');expect(remaining.Messages??[]).toHaveLength(0);

      const failedBody=request(playerId,wallet.id,'worker-dlq','1.00');const failed=await wagers.submit(failedBody,`worker-dlq-${playerId}`);
      const dlqEnvelope={eventId:crypto.randomUUID(),eventType:'WagerTransactionRequested',aggregateId:wallet.id,correlationId:failed.transactionId,occurredAt:new Date().toISOString(),version:1,data:{transactionId:failed.transactionId}};
      await client.send(new SendMessageCommand({QueueUrl:deadLetterUrl,MessageBody:JSON.stringify({...dlqEnvelope,messageId:dlqEnvelope.eventId}),MessageGroupId:wallet.id,MessageDeduplicationId:dlqEnvelope.eventId}));
      await (worker as unknown as {consumeDlqBatch:()=>Promise<void>}).consumeDlqBatch();
      const [failedRow]=await db.query<any>('select status,failure_code from wager_transactions where id=$1',[failed.transactionId]);
      const dlqRemaining=await client.send(new ReceiveMessageCommand({QueueUrl:deadLetterUrl,WaitTimeSeconds:1,VisibilityTimeout:0,MaxNumberOfMessages:1}));
      expect(failedRow).toEqual({status:'FAILED',failure_code:'MAX_RETRIES_EXCEEDED'});expect(dlqRemaining.Messages??[]).toHaveLength(0);
    } finally {
      await worker?.onModuleDestroy();
      if(previousCommandUrl===undefined)delete process.env.SQS_QUEUE_URL;else process.env.SQS_QUEUE_URL=previousCommandUrl;
      if(previousDlqUrl===undefined)delete process.env.SQS_DLQ_QUEUE_URL;else process.env.SQS_DLQ_QUEUE_URL=previousDlqUrl;
      if(commandUrl)await client.send(new DeleteQueueCommand({QueueUrl:commandUrl}));
      if(deadLetterUrl)await client.send(new DeleteQueueCommand({QueueUrl:deadLetterUrl}));
      client.destroy();
    }
  },30_000);

  test('outbox publisher retries transient send errors and two instances publish safely',async()=>{
    if(!process.env.SQS_ENDPOINT)throw new Error('SQS_ENDPOINT is required for the outbox integration test');
    const client=new SQSClient({region:process.env.AWS_REGION??'us-east-1',endpoint:process.env.SQS_ENDPOINT,credentials:{accessKeyId:process.env.AWS_ACCESS_KEY_ID??'test',secretAccessKey:process.env.AWS_SECRET_ACCESS_KEY??'test'}});
    const suffix=crypto.randomUUID();let queueUrl:string|undefined;let publisherA:QueueWorkers|undefined;let publisherB:QueueWorkers|undefined;
    const previousCommandUrl=process.env.SQS_QUEUE_URL;
    try {
      queueUrl=(await client.send(new CreateQueueCommand({QueueName:`outbox-publisher-${suffix}.fifo`,Attributes:{FifoQueue:'true',ContentBasedDeduplication:'false'}}))).QueueUrl;
      if(!queueUrl)throw new Error('SQS did not return isolated queue URL');
      const playerId=makePlayer();const wallet=await wallets.create(playerId,{amount:'0.00',currency:'BRL'});
      const submitted=await wagers.submit(request(playerId,wallet.id,'publisher','1.00'),`publisher-${playerId}`);
      const [outbox]=await db.query<any>(`select id,payload from outbox_messages where event_type='WagerTransactionRequested' and payload->'data'->>'transactionId'=$1`,[submitted.transactionId]);
      await db.query('update outbox_messages set next_attempt_at=now()+interval \'1 hour\' where published_at is null and aggregate_id<>$1',[wallet.id]);

      process.env.SQS_QUEUE_URL=`${process.env.SQS_ENDPOINT}/000000000000/missing-${suffix}.fifo`;
      publisherA=new QueueWorkers(db,wagers,metrics);publisherB=new QueueWorkers(db,wagers,metrics);
      await (publisherA as unknown as {publishBatch:()=>Promise<void>}).publishBatch();
      const [afterFailure]=await db.query<any>('select attempts,published_at from outbox_messages where id=$1',[outbox.id]);
      expect(afterFailure.attempts).toBe(1);expect(afterFailure.published_at).toBeNull();

      process.env.SQS_QUEUE_URL=queueUrl;
      await db.query('update outbox_messages set next_attempt_at=now() where id=$1',[outbox.id]);
      await Promise.all([
        (publisherA as unknown as {publishBatch:()=>Promise<void>}).publishBatch(),
        (publisherB as unknown as {publishBatch:()=>Promise<void>}).publishBatch(),
      ]);
      const [published]=await db.query<any>('select attempts,published_at from outbox_messages where id=$1',[outbox.id]);
      const messages=await client.send(new ReceiveMessageCommand({QueueUrl:queueUrl,WaitTimeSeconds:1,VisibilityTimeout:0,MaxNumberOfMessages:10}));
      const event=messages.Messages?.find(message=>JSON.parse(message.Body??'{}').eventId===outbox.payload.eventId);
      expect(published.attempts).toBe(1);expect(published.published_at).not.toBeNull();expect(event).toBeDefined();
    } finally {
      await Promise.all([publisherA?.onModuleDestroy(),publisherB?.onModuleDestroy()]);
      if(previousCommandUrl===undefined)delete process.env.SQS_QUEUE_URL;else process.env.SQS_QUEUE_URL=previousCommandUrl;
      if(queueUrl)await client.send(new DeleteQueueCommand({QueueUrl:queueUrl}));
      client.destroy();
    }
  },30_000);
});
