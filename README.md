# Distributed Wagering Processor

Serviço financeiro assíncrono para processar apostas de provedores com idempotência persistente, saldo materializado, ledger auditável e publicação via transactional outbox.

## Requisitos

- Docker Compose v2
- Bun 1.x (ou execute pela imagem Bun do Compose)

## Subir o ambiente

```sh
docker compose up
```

A API fica em `http://localhost:3000`; PostgreSQL em `localhost:5432`; LocalStack SQS em `localhost:4566`. A aplicação executa as migrations antes de iniciar. O serviço `queue-init` cria as filas FIFO.

## Exemplos

```powershell
curl.exe -X POST http://localhost:3000/wallets -H "content-type: application/json" -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"100.00","currency":"BRL"}}'

curl.exe -X POST http://localhost:3000/wagering/transactions -H "content-type: application/json" -H "idempotency-key: provider-a:transaction-123" -d '{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"WALLET_UUID","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
```

O endpoint de submissão retorna `202 Accepted` com status `PENDING`. Consulte `GET /wagering/transactions/:transactionId` para observar o resultado; o processamento ocorre pelo SQS. A API usa `201` para wallet criada, `409` para conflitos de wallet/idempotência, `422` para payload inválido, `404` para recurso inexistente e `503` para falha transitória ao aceitar uma operação. Rejeições de regra de negócio chegam como estado `REJECTED` com `failureCode` na consulta; falhas permanentes do worker ficam `FAILED` após a DLQ.

## API

- `POST /wallets`, `GET /wallets/:walletId`, `GET /wallets/:walletId/ledger?cursor=&limit=`
- `POST /wallets/:walletId/reconciliation`
- `POST /wagering/transactions` (requer `Idempotency-Key`)
- `GET /wagering/transactions/:transactionId`
- `GET /providers/:providerId/wagering/transactions/:externalTransactionId`
- `GET /health/live`, `GET /health/ready`

## Testes e migrations

```sh
bun install
bun run typecheck
bun run test
bun run migration:up
bun run migration:down
```

No PowerShell, defina `DATABASE_URL_TEST` antes de `bun run test:integration`; use um banco isolado e descartável.

Os testes de integração usam PostgreSQL e LocalStack reais; consulte `tests/README.md` para o banco isolado e execução. Nunca aponte `DATABASE_URL_TEST` para dados que não possam ser descartados.

## Operação e limitações conhecidas

Logs operacionais são JSON sem valores financeiros. Rejeições usam `failureCode` estável. As filas recebem redelivery e mandam mensagens após cinco recebimentos para DLQ. `ARCHITECTURE.md` registra consistência, concorrência, políticas de retry e extensões pendentes.
