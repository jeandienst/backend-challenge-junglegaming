import { Migration } from '@mikro-orm/migrations';

export class Migration202611110001 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`create extension if not exists pgcrypto;`);
    this.addSql(`create table wallets (
      id uuid primary key default gen_random_uuid(), player_id uuid not null,
      currency char(3) not null check (currency ~ '^[A-Z]{3}$'),
      balance numeric(20,2) not null default 0 check (balance >= 0),
      version integer not null default 1 check (version >= 1),
      created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
      unique(player_id, currency), unique(id, player_id, currency), unique(id,currency)
    );`);
    this.addSql(`create table wager_transactions (
      id uuid primary key default gen_random_uuid(), provider_id text not null,
      external_transaction_id text not null, idempotency_key text not null,
      payload_hash char(64) not null check(payload_hash ~ '^[0-9a-f]{64}$'), wallet_id uuid not null,
      player_id uuid not null, currency char(3) not null check(currency ~ '^[A-Z]{3}$'), round_id text not null, game_id text not null,
      kind text not null check(kind in ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
      amount numeric(20,2) not null check(amount > 0), reference_external_transaction_id text,
      reference_transaction_id uuid references wager_transactions(id),
      status text not null check(status in ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
      failure_code text, attempts integer not null default 0 check(attempts>=0), next_attempt_at timestamptz,
      balance_snapshot numeric(20,2) check(balance_snapshot is null or balance_snapshot>=0), created_at timestamptz not null default now(), processed_at timestamptz,
      unique(provider_id, external_transaction_id), unique(provider_id, idempotency_key), unique(id,wallet_id),
      check ((kind in ('REFUND','ROLLBACK') and reference_external_transaction_id is not null) or (kind not in ('REFUND','ROLLBACK')))
    );`);
    this.addSql(`create index wager_pending_reference_idx on wager_transactions(next_attempt_at, created_at) where status='PENDING_REFERENCE';`);
    this.addSql(`create unique index wager_single_refund_idx on wager_transactions(reference_transaction_id) where kind='REFUND';`);
    this.addSql(`create unique index wager_single_rollback_idx on wager_transactions(reference_transaction_id) where kind='ROLLBACK';`);
    this.addSql(`create table wallet_ledger (
      id uuid primary key default gen_random_uuid(), wallet_id uuid not null references wallets(id),
      transaction_id uuid not null references wager_transactions(id), direction text not null check(direction in ('DEBIT','CREDIT')),
      amount numeric(20,2) not null check(amount > 0), currency char(3) not null,
      balance_before numeric(20,2) not null check(balance_before >= 0),
      balance_after numeric(20,2) not null check(balance_after >= 0), created_at timestamptz not null default now(),
      unique(wallet_id, transaction_id), foreign key(transaction_id,wallet_id) references wager_transactions(id,wallet_id), foreign key(wallet_id,currency) references wallets(id,currency), check(currency ~ '^[A-Z]{3}$'),
      check ((direction='CREDIT' and balance_after=balance_before+amount) or (direction='DEBIT' and balance_after=balance_before-amount))
    );`);
    this.addSql(`create index wallet_ledger_cursor_idx on wallet_ledger(wallet_id, created_at desc, id desc);`);
    this.addSql(`create table inbox_messages (consumer_name text not null, message_id text not null, payload_hash char(64) not null check(payload_hash ~ '^[0-9a-f]{64}$'), received_at timestamptz not null default now(), processed_at timestamptz, primary key(consumer_name,message_id));`);
    this.addSql(`create table outbox_messages (
      id uuid primary key default gen_random_uuid(), aggregate_id uuid not null, event_type text not null,
      payload jsonb not null, occurred_at timestamptz not null default now(), attempts integer not null default 0 check(attempts>=0),
      next_attempt_at timestamptz not null default now(), published_at timestamptz, locked_until timestamptz,
      check(payload ? 'eventId'), check(payload->>'eventType'=event_type)
    );`);
    this.addSql(`create index outbox_due_idx on outbox_messages(next_attempt_at, occurred_at) where published_at is null;`);
    this.addSql(`create unique index outbox_event_id_idx on outbox_messages((payload->>'eventId'));`);
    this.addSql(`create function prevent_outbox_payload_mutation() returns trigger language plpgsql as $$ begin
      if (new.aggregate_id,new.event_type,new.payload,new.occurred_at) is distinct from (old.aggregate_id,old.event_type,old.payload,old.occurred_at) then raise exception 'outbox event payload is immutable'; end if;
      return new; end $$;`);
    this.addSql(`create trigger outbox_payload_immutable before update on outbox_messages for each row execute function prevent_outbox_payload_mutation();`);
    this.addSql(`create function prevent_ledger_mutation() returns trigger language plpgsql as $$ begin raise exception 'wallet ledger is immutable'; end $$;`);
    this.addSql(`create trigger wallet_ledger_immutable before update or delete on wallet_ledger for each row execute function prevent_ledger_mutation();`);
    this.addSql(`create function validate_ledger_identity() returns trigger language plpgsql as $$ begin
      if not exists(select 1 from wager_transactions t join wallets w on w.id=t.wallet_id where t.id=new.transaction_id and t.wallet_id=new.wallet_id and t.currency=new.currency and t.player_id=w.player_id and t.status='PROCESSED') then
        raise exception 'ledger transaction context does not match wallet';
      end if; return new; end $$;`);
    this.addSql(`create constraint trigger wallet_ledger_identity after insert on wallet_ledger deferrable initially deferred for each row execute function validate_ledger_identity();`);
    this.addSql(`create function validate_wager_ledger_status() returns trigger language plpgsql as $$ begin
      if new.status='PROCESSED' and new.kind<>'LOSS' and not exists(select 1 from wallet_ledger where transaction_id=new.id and wallet_id=new.wallet_id) then raise exception 'processed balance transaction requires a ledger entry'; end if;
      if new.status='PROCESSED' and new.kind='LOSS' and exists(select 1 from wallet_ledger where transaction_id=new.id) then raise exception 'LOSS cannot have a ledger entry'; end if;
      if new.status in ('REJECTED','FAILED') and exists(select 1 from wallet_ledger where transaction_id=new.id) then raise exception 'rejected or failed transaction cannot have a ledger entry'; end if;
      return new; end $$;`);
    this.addSql(`create constraint trigger wager_status_matches_ledger after insert or update on wager_transactions deferrable initially deferred for each row execute function validate_wager_ledger_status();`);
    this.addSql(`create function prevent_terminal_wager_mutation() returns trigger language plpgsql as $$ begin
      if tg_op='DELETE' then raise exception 'wager transaction records are immutable'; end if;
      if old.status in ('PROCESSED','REJECTED','FAILED') then raise exception 'terminal wager transaction is immutable'; end if;
      if new.status is distinct from old.status and not ((old.status='PENDING' and new.status in ('PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')) or (old.status='PENDING_REFERENCE' and new.status in ('PROCESSED','REJECTED','FAILED'))) then raise exception 'invalid wager transaction state transition'; end if;
      if (new.id,new.provider_id,new.external_transaction_id,new.idempotency_key,new.payload_hash,new.wallet_id,new.player_id,new.currency,new.round_id,new.game_id,new.kind,new.amount,new.reference_external_transaction_id)
        is distinct from (old.id,old.provider_id,old.external_transaction_id,old.idempotency_key,old.payload_hash,old.wallet_id,old.player_id,old.currency,old.round_id,old.game_id,old.kind,old.amount,old.reference_external_transaction_id) then raise exception 'wager transaction identity is immutable'; end if;
      return new; end $$;`);
    this.addSql(`create trigger wager_transaction_immutable_fields before update or delete on wager_transactions for each row execute function prevent_terminal_wager_mutation();`);
    this.addSql(`create function validate_wallet_ledger_balance() returns trigger language plpgsql as $$ declare target uuid; stored numeric(20,2); calculated numeric(20,2); begin
      if tg_table_name='wallets' then target:=(to_jsonb(new)->>'id')::uuid; else target:=(to_jsonb(new)->>'wallet_id')::uuid; end if;
      select w.balance,coalesce(sum(case when l.direction='CREDIT' then l.amount else -l.amount end),0)::numeric(20,2) into stored,calculated from wallets w left join wallet_ledger l on l.wallet_id=w.id where w.id=target group by w.id,w.balance;
      if stored is not null and stored<>calculated then raise exception 'wallet balance does not match immutable ledger'; end if;
      return new; end $$;`);
    this.addSql(`create constraint trigger wallet_balance_matches_ledger after insert or update on wallets deferrable initially deferred for each row execute function validate_wallet_ledger_balance();`);
    this.addSql(`create constraint trigger ledger_balance_matches_wallet after insert on wallet_ledger deferrable initially deferred for each row execute function validate_wallet_ledger_balance();`);
    this.addSql(`create function validate_wallet_version() returns trigger language plpgsql as $$ begin
      if new.balance is distinct from old.balance then
        if new.version<>old.version+1 then raise exception 'wallet version must increment exactly once with a balance change'; end if;
      elsif new.version<>old.version then raise exception 'wallet version cannot change without a balance change'; end if;
      return new; end $$;`);
    this.addSql(`create trigger wallet_version_matches_balance before update on wallets for each row execute function validate_wallet_version();`);
    this.addSql(`create function validate_wallet_initial_version() returns trigger language plpgsql as $$ begin if new.version<>1 then raise exception 'wallet version must start at 1'; end if; return new; end $$;`);
    this.addSql(`create trigger wallet_initial_version before insert on wallets for each row execute function validate_wallet_initial_version();`);
  }
  override async down(): Promise<void> {
    this.addSql(`drop table if exists outbox_messages, inbox_messages, wallet_ledger, wager_transactions, wallets cascade; drop function if exists prevent_ledger_mutation(); drop function if exists validate_ledger_identity(); drop function if exists validate_wager_ledger_status(); drop function if exists prevent_terminal_wager_mutation(); drop function if exists validate_wallet_ledger_balance(); drop function if exists validate_wallet_version(); drop function if exists validate_wallet_initial_version(); drop function if exists prevent_outbox_payload_mutation();`);
  }
}
