import { Database } from '../../src/infra/database';
import { Metrics } from '../../src/infra/metrics';
import { WagerService } from '../../src/services/wager.service';

const main=async()=>{const id=process.env.WAGER_TRANSACTION_ID;if(!id)throw new Error('WAGER_TRANSACTION_ID is required');const messageId=process.env.WAGER_MESSAGE_ID;const payloadHash=process.env.WAGER_PAYLOAD_HASH;const db=new Database();await db.onModuleInit();try{await new WagerService(db,new Metrics()).process(id,messageId&&payloadHash?{messageId,payloadHash}:undefined);}finally{await db.onModuleDestroy();}};
await main();
