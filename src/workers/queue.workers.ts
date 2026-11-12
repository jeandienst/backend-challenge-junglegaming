import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { SQSClient, ReceiveMessageCommand, DeleteMessageCommand, SendMessageCommand, GetQueueAttributesCommand, ChangeMessageVisibilityCommand } from '@aws-sdk/client-sqs';
import { createHash } from 'node:crypto';
import { Database } from '../infra/database';
import { WagerService } from '../services/wager.service';
import { Metrics } from '../infra/metrics';
import { logEvent } from '../infra/logging';

@Injectable()
export class QueueWorkers implements OnModuleInit, OnModuleDestroy {
  private readonly sqs=new SQSClient({region:process.env.AWS_REGION??'us-east-1',endpoint:process.env.SQS_ENDPOINT,credentials:{accessKeyId:process.env.AWS_ACCESS_KEY_ID??'test',secretAccessKey:process.env.AWS_SECRET_ACCESS_KEY??'test'}});
  private stopped=false; private readonly timers: ReturnType<typeof setInterval>[]=[];
  private readonly active=new Set<Promise<unknown>>();
  constructor(private readonly db:Database,private readonly wagers:WagerService,private readonly metrics:Metrics){}
  onModuleInit() {
    this.timers.push(setInterval(()=>this.track(this.publishBatch()),500));
    this.timers.push(setInterval(()=>this.track(this.consumeBatch()),500));
    this.timers.push(setInterval(()=>this.track(this.consumeDlqBatch()),1000));
    this.timers.push(setInterval(()=>this.track(this.wagers.retryPendingReferences()),5000));
    this.timers.push(setInterval(()=>this.track(this.sampleOperationalMetrics()),10000));
  }
  async onModuleDestroy(){this.stopped=true;for(const t of this.timers)clearInterval(t);await Promise.allSettled([...this.active]);this.sqs.destroy();}
  private track(task:Promise<unknown>){this.active.add(task);void task.catch(()=>undefined).finally(()=>this.active.delete(task));}
  private async publishBatch(){
    if(this.stopped)return;
    try {
      const rows=await this.db.query<any>(`with due as (select id from outbox_messages where published_at is null and next_attempt_at<=now() and (locked_until is null or locked_until<now()) order by occurred_at limit 20 for update skip locked) update outbox_messages o set locked_until=now()+interval '30 seconds' from due where o.id=due.id returning o.id,o.event_type,o.aggregate_id,o.payload,o.attempts`);
      for(const row of rows){
        try {
          const request=row.event_type==='WagerTransactionRequested';
          const queueUrl=request?process.env.SQS_QUEUE_URL:process.env.SQS_EVENTS_QUEUE_URL;
          if(!queueUrl)throw new Error('SQS queue URL missing');
          await this.sqs.send(new SendMessageCommand({QueueUrl:queueUrl,MessageBody:JSON.stringify({...row.payload,messageId:row.payload.eventId,...(request?{type:'WagerTransactionRequested'}:{})}),MessageGroupId:String(row.aggregate_id),MessageDeduplicationId:String(row.payload.eventId)}));
          await this.db.query('update outbox_messages set published_at=now(),locked_until=null where id=$1',[row.id]);
        }catch(error){
          const attempts=Number(row.attempts)+1; const delay=Math.min(1000*2**Math.min(attempts,10),900000);
          this.metrics.inc('wager_retries_total',{reason:'outbox_publish'});
          await this.db.query(`update outbox_messages set attempts=$2,next_attempt_at=now()+($3::text||' milliseconds')::interval,locked_until=null where id=$1`,[row.id,attempts,delay]);
          logEvent('error',{event:'outbox.publish_failed',correlationId:row.payload.correlationId??null,messageId:row.payload.eventId??row.id,transactionId:(row.payload.data as any)?.transactionId??null,walletId:row.aggregate_id,providerId:(row.payload.data as any)?.providerId??null,attempts});
        }
      }
    } catch { /* readiness reports the dependency failure; next tick retries */ }
  }
  private async consumeBatch(){
    if(this.stopped)return;
    try {
      const result=await this.sqs.send(new ReceiveMessageCommand({QueueUrl:process.env.SQS_QUEUE_URL!,MaxNumberOfMessages:10,WaitTimeSeconds:10,VisibilityTimeout:30,MessageSystemAttributeNames:['ApproximateReceiveCount']}));
      for(const message of result.Messages??[]){
        let body:any={};let transactionId:string|null=null;
        try {
          body=JSON.parse(message.Body??'{}'); transactionId=body?.data?.transactionId??null;
          if(body?.eventType!=='WagerTransactionRequested'||typeof transactionId!=='string')throw new Error('invalid_internal_message');
          const messageId=body.messageId??body.eventId??message.MessageId;
          const payloadHash=createHash('sha256').update(JSON.stringify(body)).digest('hex');
          await this.wagers.verifyQueuedRequest(transactionId,body.data);
          await this.wagers.process(transactionId,{messageId,payloadHash});
          await this.sqs.send(new DeleteMessageCommand({QueueUrl:process.env.SQS_QUEUE_URL!,ReceiptHandle:message.ReceiptHandle}));
        }catch(error){
          this.metrics.inc('wager_retries_total',{reason:'queue_message'});
          const receives=Number(message.Attributes?.ApproximateReceiveCount??1);const delay=Math.min(5*2**Math.min(receives,8),900);
          if(message.ReceiptHandle)try{await this.sqs.send(new ChangeMessageVisibilityCommand({QueueUrl:process.env.SQS_QUEUE_URL!,ReceiptHandle:message.ReceiptHandle,VisibilityTimeout:delay}));}catch{}
          logEvent('error',{event:'queue.message_failed',correlationId:body?.correlationId??null,messageId:body?.messageId??message.MessageId??null,transactionId,walletId:body?.aggregateId??body?.data?.walletId??null,providerId:body?.data?.providerId??null,retryDelaySeconds:delay});
        }
      }
    }catch { /* queue temporarily unavailable; visibility/long polling retry */ }
  }
  private async consumeDlqBatch(){
    if(this.stopped)return;
    try {
      const result=await this.sqs.send(new ReceiveMessageCommand({QueueUrl:process.env.SQS_DLQ_QUEUE_URL!,MaxNumberOfMessages:10,WaitTimeSeconds:5,VisibilityTimeout:30,MessageSystemAttributeNames:['ApproximateReceiveCount']}));
      for(const message of result.Messages??[]){
        let body:any={};
        try {
          body=JSON.parse(message.Body??'{}');const transactionId=body?.data?.transactionId;
          if(typeof transactionId!=='string')throw new Error('invalid_dlq_message');
          const messageId=body.messageId??body.eventId??message.MessageId!;
          const payloadHash=createHash('sha256').update(JSON.stringify(body)).digest('hex');
          await this.wagers.fail(transactionId,{messageId,payloadHash});
          await this.sqs.send(new DeleteMessageCommand({QueueUrl:process.env.SQS_DLQ_QUEUE_URL!,ReceiptHandle:message.ReceiptHandle}));
          this.metrics.inc('wager_dlq_messages_processed_total');
        }catch{this.metrics.inc('wager_retries_total',{reason:'dlq_persistence'});const receives=Number(message.Attributes?.ApproximateReceiveCount??1);const delay=Math.min(5*2**Math.min(receives,8),900);if(message.ReceiptHandle)try{await this.sqs.send(new ChangeMessageVisibilityCommand({QueueUrl:process.env.SQS_DLQ_QUEUE_URL!,ReceiptHandle:message.ReceiptHandle,VisibilityTimeout:delay}));}catch{}logEvent('error',{event:'queue.dlq_persist_failed',correlationId:body?.correlationId??null,messageId:body?.messageId??message.MessageId??null,transactionId:body?.data?.transactionId??null,walletId:body?.aggregateId??null,providerId:body?.data?.providerId??null,retryDelaySeconds:delay});}
      }
    }catch{}
  }
  async ready(){
    await this.db.query('select 1');
    await this.sqs.send(new GetQueueAttributesCommand({QueueUrl:process.env.SQS_QUEUE_URL!,AttributeNames:['QueueArn']}));
    return true;
  }
  async sampleOperationalMetrics(){
    try {
      const [lag]=await this.db.query<any>(`select coalesce(extract(epoch from (now()-min(occurred_at))),0) as seconds from outbox_messages where published_at is null`);
      this.metrics.gauge('wager_outbox_lag_seconds',Number(lag?.seconds??0));
      const attrs=await this.sqs.send(new GetQueueAttributesCommand({QueueUrl:process.env.SQS_DLQ_QUEUE_URL!,AttributeNames:['ApproximateNumberOfMessages','ApproximateNumberOfMessagesNotVisible']}));
      this.metrics.gauge('wager_dlq_messages',Number(attrs.Attributes?.ApproximateNumberOfMessages??0)+Number(attrs.Attributes?.ApproximateNumberOfMessagesNotVisible??0));
    }catch{}
  }
}
