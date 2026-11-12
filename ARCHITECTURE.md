# Decisões de arquitetura

## Limites e stack

NestJS no runtime Bun, TypeScript estrito, MikroORM/PostgreSQL e AWS SQS via LocalStack. MikroORM fornece a fronteira transacional e a conexão PostgreSQL; queries críticas usam SQL explícito para tornar visíveis locks, constraints, leases e `SKIP LOCKED`. Não há Prisma.

`Money` guarda centavos em `bigint`; contratos e colunas SQL usam decimal de escala 2. Nenhum cálculo financeiro converte para `number`. A moeda permanece no objeto e operações entre moedas diferentes falham. A validação de entrada aceita valores não negativos, exatamente duas casas e códigos presentes na lista de moedas ISO provida por `Intl`.

## Consistência financeira

Cada wallet é serializada por `SELECT ... FOR UPDATE` na linha daquela wallet. Isso permite wallets distintas em paralelo e funciona entre processos sem lock em memória. A transação PostgreSQL inclui status, saldo, ledger e eventos outbox. Constraints cobrem unicidade da wallet por player/moeda, idempotência por provider/key, identificador externo, uma reversão por tipo/referência, um lançamento por transação/wallet, aritmética do ledger e saldo não negativo. Triggers diferidos comparam saldo e soma do ledger no commit, validam a identidade entre lançamento, transação e wallet e impõem a regra de versão. Outros triggers impedem update/delete no ledger e mudança de dados ou transição terminal de uma transação terminal.

BET insuficiente é rejeitada com `INSUFFICIENT_FUNDS`. Débito de reversão sem saldo usa `REVERSAL_WOULD_OVERDRAW`. Referências pendentes têm backoff exponencial limitado a 12 tentativas; ao esgotar, a transação é rejeitada como `REFERENCE_NOT_FOUND`. Mensagens da mesma wallet são ordenadas com FIFO como otimização; o lock e as constraints são a garantia final.

## Idempotência e fila

O header é a chave autoritativa; o hash SHA-256 usa JSON canônico dos campos do domínio, excluindo header e transporte. A chave e o identificador externo são únicos no banco por provider. Repetição idêntica devolve o status e snapshot de saldo gravado; payload divergente produz conflito.

A submissão persiste transação PENDING e evento `WagerTransactionRequested` na mesma SQL. Uma repetição idêntica enquanto a operação está pendente retorna seu estado persistido; após um resultado terminal, retorna o status, o código de falha quando houver e o snapshot de saldo gravados para aquela operação. `idempotentReplay` identifica a repetição, e payload divergente produz conflito. Publisher reclama lotes com lease e `FOR UPDATE SKIP LOCKED`; evento enviado antes de marcar outbox pode ser duplicado, então SQS usa deduplication ID e o consumidor grava inbox persistente no commit financeiro. ACK ocorre após retorno do use case. Erros transitórios recebem visibilidade com backoff exponencial; a redrive policy manda mensagens após cinco recebimentos à DLQ, onde são auditadas como `FAILED`. Eventos de integração financeiros usam fila separada para não serem consumidos como comandos.

## Eventos e workers

Cada evento usa envelope versionado com `eventId`, `eventType`, `aggregateId`, correlação, causa, timestamp e `data`. `WalletBalanceChanged` só é gravado quando existe movimento; `LOSS` produz apenas `WagerTransactionProcessed`; rejeições não alteram saldo. O publisher mantém retry com backoff e lease expirável para recuperação após crash.

## HTTP e autenticação

Autenticação foi omitida dentro do timebox: não vale pontos no desafio e o texto permite essa opção. `NoopAuthGuard` e `ProviderIdentityPort` deixam o ponto de extensão explícito; em produção adotaria OIDC com Keycloak ou Zitadel, validando issuer, audience e identidade do provedor. Health permanece público. Canal SQS é tratado como interno, mas valores do domínio são validados.

## Observabilidade e trade-offs

Health liveness/readiness são separados. Falhas do worker são registradas em JSON sem payload financeiro. `/health/metrics` expõe contadores por status, duplicatas, retries, DLQ, contenção/espera de locks, outbox lag e latência. A métrica de contenção é inferida de espera de consulta por wallet/transação acima de 5 ms. Reconciliação compara soma do ledger ao saldo, incrementa métrica e registra sinal sem valores financeiros; não corrige dados. O worker de polling é simples e escala horizontalmente pelas leases no banco.

## Escopo a completar antes de produção

As métricas expostas em `/health/metrics` são locais ao processo; em produção devem ser exportadas e agregadas por Prometheus, com tracing OTel opcional. Os testes de corrida usam múltiplos processos Bun e PostgreSQL real; o teste SQS requer LocalStack real. O banco aplica trigger diferido para exigir igualdade entre saldo materializado e ledger no commit, além de constraints de identidade e imutabilidade.
