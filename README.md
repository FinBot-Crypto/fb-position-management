# fb-position-monitor

Monitora posições abertas e fecha automaticamente quando condições são atingidas.

## Fluxo

```
trade.executed (fb-execution)
  → registra posição no KV store

Loop a cada 10s:
  → preço ≤ SL? → fecha STOP_LOSS
  → preço ≥ TP? → fecha TAKE_PROFIT
  → RSI ≥ 70? → fecha RSI_REVERSAL
  → hold > 48h? → fecha TIME_EXIT
  → trailing stop (move SL conforme lucro sobe)
  → trade.closed
```

## Variáveis de Ambiente

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `NATS_URL` | `nats://crypto-nats:4222` | Servidor NATS |
| `MONITOR_INTERVAL` | `10` | Intervalo de verificação (segundos) |
| `MAX_HOLD_HOURS` | `48` | Tempo máximo com posição aberta |
| `RSI_EXIT` | `70` | RSI para fechar por reversal |
| `TRAILING_STOP` | `true` | Ativa trailing stop |
| `TRAILING_ATR` | `2.0` | Distância do trailing stop em ATR |
| `TRAILING_ACTIVATION` | `1.0` | ATRs de lucro para ativar trailing |
| `DRY_RUN` | `true` | Modo simulação |
| `BINANCE_API_KEY` | | Chave API Binance |
| `BINANCE_API_SECRET` | | Secret API Binance |

## Deploy

```bash
docker run -e NATS_URL=nats://crypto-nats:4222 \
  -e DRY_RUN=false \
  -e BINANCE_API_KEY=xxx -e BINANCE_API_SECRET=xxx \
  fb-position-management:latest
```
