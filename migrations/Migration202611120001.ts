import { Migration } from '@mikro-orm/migrations';

/** Keep the LOSS no-ledger invariant enforced even for direct SQL writes. */
export class Migration202611120001 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`create function prevent_loss_ledger_entry() returns trigger language plpgsql as $$ begin
      if exists(select 1 from wager_transactions where id=new.transaction_id and kind='LOSS') then
        raise exception 'LOSS cannot have a ledger entry';
      end if;
      return new;
    end $$;`);
    this.addSql(`create constraint trigger wallet_ledger_no_loss after insert on wallet_ledger deferrable initially deferred for each row execute function prevent_loss_ledger_entry();`);
  }

  override async down(): Promise<void> {
    this.addSql(`drop trigger if exists wallet_ledger_no_loss on wallet_ledger;`);
    this.addSql(`drop function if exists prevent_loss_ledger_entry();`);
  }
}
