export interface MoneyProps { amount: string; currency: string }
const DECIMAL = /^(0|[1-9]\d{0,17})\.\d{2}$/;
const ISO_CURRENCY_CODES = new Set(Intl.supportedValuesOf('currency'));

export class Money {
  private constructor(private readonly cents: bigint, public readonly currency: string) {}
  static from(props: MoneyProps): Money {
    if (!ISO_CURRENCY_CODES.has(props.currency) || !DECIMAL.test(props.amount)) throw new Error('INVALID_MONEY');
    const [whole, fraction] = props.amount.split('.');
    return new Money(BigInt(whole!) * 100n + BigInt(fraction!), props.currency);
  }
  static zero(currency: string): Money { return Money.from({ amount: '0.00', currency }); }
  private same(other: Money): void { if (other.currency !== this.currency) throw new Error('CURRENCY_MISMATCH'); }
  add(other: Money): Money { this.same(other); return new Money(this.cents + other.cents, this.currency); }
  subtract(other: Money): Money { this.same(other); return new Money(this.cents - other.cents, this.currency); }
  negate(): Money { return new Money(-this.cents, this.currency); }
  isZero(): boolean { return this.cents === 0n; }
  isPositive(): boolean { return this.cents > 0n; }
  isNegative(): boolean { return this.cents < 0n; }
  isLessThan(other: Money): boolean { this.same(other); return this.cents < other.cents; }
  equals(other: Money): boolean { return this.currency === other.currency && this.cents === other.cents; }
  toJSON(): MoneyProps { const sign = this.cents < 0n ? '-' : ''; const n = this.cents < 0n ? -this.cents : this.cents; return { amount: `${sign}${n / 100n}.${String(n % 100n).padStart(2,'0')}`, currency: this.currency }; }
  toString(): string { return `${this.toJSON().amount} ${this.currency}`; }
}
