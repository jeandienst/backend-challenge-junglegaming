import { describe, expect, test } from 'bun:test';
import { CreateQueueCommand, DeleteMessageCommand, DeleteQueueCommand, GetQueueAttributesCommand, ReceiveMessageCommand, SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';

const enabled=Boolean(process.env.SQS_ENDPOINT&&process.env.SQS_QUEUE_URL);
describe.skipIf(!enabled)('LocalStack SQS FIFO integration',()=>{
  test('accepts and redelivers FIFO messages from the real queue',async()=>{
    const client=new SQSClient({region:process.env.AWS_REGION??'us-east-1',endpoint:process.env.SQS_ENDPOINT,credentials:{accessKeyId:'test',secretAccessKey:'test'}});
    const queueName=`integration-probe-${crypto.randomUUID()}.fifo`;
    const {QueueUrl}=await client.send(new CreateQueueCommand({QueueName:queueName,Attributes:{FifoQueue:'true',ContentBasedDeduplication:'false'}}));
    if(!QueueUrl)throw new Error('SQS did not return a queue URL');
    try {
      const body=JSON.stringify({messageId:crypto.randomUUID(),type:'IntegrationProbe'});
      await client.send(new SendMessageCommand({QueueUrl,MessageBody:body,MessageGroupId:'test',MessageDeduplicationId:crypto.randomUUID()}));
      const first=await client.send(new ReceiveMessageCommand({QueueUrl,WaitTimeSeconds:5,VisibilityTimeout:0,MaxNumberOfMessages:1}));
      const firstMatch=first.Messages?.find(message=>message.Body===body);
      expect(firstMatch).toBeDefined();
      const second=await client.send(new ReceiveMessageCommand({QueueUrl,WaitTimeSeconds:5,VisibilityTimeout:0,MaxNumberOfMessages:1}));
      const redelivery=second.Messages?.find(message=>message.Body===body);
      expect(redelivery).toBeDefined();
      if(redelivery?.ReceiptHandle)await client.send(new DeleteMessageCommand({QueueUrl,ReceiptHandle:redelivery.ReceiptHandle}));
    } finally {
      await client.send(new DeleteQueueCommand({QueueUrl}));
      client.destroy();
    }
  },15_000);

  test('redrives a repeatedly received FIFO message to its dead-letter queue',async()=>{
    const client=new SQSClient({region:process.env.AWS_REGION??'us-east-1',endpoint:process.env.SQS_ENDPOINT,credentials:{accessKeyId:'test',secretAccessKey:'test'}});
    const suffix=crypto.randomUUID();const deadLetterName=`integration-dlq-${suffix}.fifo`;const sourceName=`integration-source-${suffix}.fifo`;
    let deadLetterUrl:string|undefined;let sourceUrl:string|undefined;
    try {
      deadLetterUrl=(await client.send(new CreateQueueCommand({QueueName:deadLetterName,Attributes:{FifoQueue:'true',ContentBasedDeduplication:'false'}}))).QueueUrl;
      if(!deadLetterUrl)throw new Error('SQS did not return a DLQ URL');
      const attributes=await client.send(new GetQueueAttributesCommand({QueueUrl:deadLetterUrl,AttributeNames:['QueueArn']}));
      const deadLetterArn=attributes.Attributes?.QueueArn;if(!deadLetterArn)throw new Error('SQS did not return a DLQ ARN');
      sourceUrl=(await client.send(new CreateQueueCommand({QueueName:sourceName,Attributes:{FifoQueue:'true',ContentBasedDeduplication:'false',VisibilityTimeout:'0',RedrivePolicy:JSON.stringify({deadLetterTargetArn:deadLetterArn,maxReceiveCount:'3'})}}))).QueueUrl;
      if(!sourceUrl)throw new Error('SQS did not return a source queue URL');
      const body=JSON.stringify({messageId:crypto.randomUUID(),type:'RetryProbe'});
      await client.send(new SendMessageCommand({QueueUrl:sourceUrl,MessageBody:body,MessageGroupId:'retry-test',MessageDeduplicationId:crypto.randomUUID()}));
      for(let receive=0;receive<5;receive++)await client.send(new ReceiveMessageCommand({QueueUrl:sourceUrl,WaitTimeSeconds:1,VisibilityTimeout:0,MaxNumberOfMessages:1}));
      const moved=await client.send(new ReceiveMessageCommand({QueueUrl:deadLetterUrl,WaitTimeSeconds:2,VisibilityTimeout:0,MaxNumberOfMessages:1}));
      expect(moved.Messages?.some(message=>message.Body===body)).toBe(true);
    } finally {
      if(sourceUrl)await client.send(new DeleteQueueCommand({QueueUrl:sourceUrl}));
      if(deadLetterUrl)await client.send(new DeleteQueueCommand({QueueUrl:deadLetterUrl}));
      client.destroy();
    }
  },15_000);
});
