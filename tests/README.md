# Testes de integração

Os testes unitários não dependem de serviços externos. A suíte de integração só é habilitada quando `DATABASE_URL_TEST` aponta para um PostgreSQL isolado e descartável; ela executa as migrations e valida concorrência real entre operações paralelas.

Os testes não substituem PostgreSQL ou SQS por mocks. A suíte de concorrência cria workers em três processos Bun independentes. Use o PostgreSQL e LocalStack do Compose, mas execute os testes dentro da rede Docker para evitar conflito de credenciais/porta com um PostgreSQL instalado no Windows. Crie um banco isolado uma vez:

```powershell
docker compose exec postgres createdb -U wager -O wager wagering_test
docker compose exec -e DATABASE_URL_TEST=postgresql://wager:wager@postgres:5432/wagering_test api bun run test:integration
```

A integração SQS cria uma fila FIFO temporária exclusiva e a remove ao terminar; assim o worker da API não consome a mensagem de teste.
