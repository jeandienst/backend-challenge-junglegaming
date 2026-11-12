export interface IntegrationEventProps<T> { eventId:string; aggregateId:string; correlationId:string; causationId?:string; occurredAt:Date; data:T }
export abstract class IntegrationEvent<T> {
  abstract readonly eventType:string; abstract readonly version:number;
  readonly eventId:string; readonly aggregateId:string; readonly correlationId:string; readonly causationId?:string; readonly occurredAt:Date; readonly data:Readonly<T>;
  protected constructor(p:IntegrationEventProps<T>){this.eventId=p.eventId;this.aggregateId=p.aggregateId;this.correlationId=p.correlationId;this.causationId=p.causationId;this.occurredAt=p.occurredAt;this.data=deepFreeze(structuredClone(p.data));}
  toJSON(){return {eventId:this.eventId,eventType:this.eventType,aggregateId:this.aggregateId,correlationId:this.correlationId,...(this.causationId?{causationId:this.causationId}:{}),occurredAt:this.occurredAt.toISOString(),version:this.version,data:this.data};}
}
function deepFreeze<T>(value:T):Readonly<T>{if(value&&typeof value==='object'){Object.freeze(value);for(const child of Object.values(value as object))deepFreeze(child);}return value;}
type EventData = Record<string,unknown>;
export class WagerTransactionRequested extends IntegrationEvent<EventData>{readonly eventType='WagerTransactionRequested';readonly version=1;private constructor(p:IntegrationEventProps<EventData>){super(p);}static create(p:IntegrationEventProps<EventData>){return new WagerTransactionRequested(p);}}
export class WagerTransactionProcessed extends IntegrationEvent<EventData>{readonly eventType='WagerTransactionProcessed';readonly version=1;private constructor(p:IntegrationEventProps<EventData>){super(p);}static create(p:IntegrationEventProps<EventData>){return new WagerTransactionProcessed(p);}}
export class WagerTransactionRejected extends IntegrationEvent<EventData>{readonly eventType='WagerTransactionRejected';readonly version=1;private constructor(p:IntegrationEventProps<EventData>){super(p);}static create(p:IntegrationEventProps<EventData>){return new WagerTransactionRejected(p);}}
export class WagerTransactionFailed extends IntegrationEvent<EventData>{readonly eventType='WagerTransactionFailed';readonly version=1;private constructor(p:IntegrationEventProps<EventData>){super(p);}static create(p:IntegrationEventProps<EventData>){return new WagerTransactionFailed(p);}}
export class WagerTransactionPendingReference extends IntegrationEvent<EventData>{readonly eventType='WagerTransactionPendingReference';readonly version=1;private constructor(p:IntegrationEventProps<EventData>){super(p);}static create(p:IntegrationEventProps<EventData>){return new WagerTransactionPendingReference(p);}}
export class WalletBalanceChanged extends IntegrationEvent<EventData>{readonly eventType='WalletBalanceChanged';readonly version=1;private constructor(p:IntegrationEventProps<EventData>){super(p);}static create(p:IntegrationEventProps<EventData>){return new WalletBalanceChanged(p);}}
