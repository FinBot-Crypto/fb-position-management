"""
fb-position-monitor: Monitora posições abertas e decide quando fechar.

Fluxo:
  trade.executed → registra posição + INSERT no banco
  Loop a cada 10s:
    → verifica TP/SL atingido (preço)
    → verifica RSI reversal (RSI > RSI_EXIT)
    → verifica time exit (hold > MAX_HOLD_HOURS)
    → trailing stop (move SL conforme lucro)
    → se fechar: publica trade.closed, cancela ordens, UPDATE no banco
"""
import asyncio, logging, os, json, time, numpy as np, ccxt, nats, psycopg2
from nats.js.api import ConsumerConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("fb-position-monitor")

NATS_URL = os.getenv("NATS_URL", "nats://crypto-nats:4222")
MONITOR_INTERVAL = int(os.getenv("MONITOR_INTERVAL", "10"))
MAX_HOLD_HOURS = float(os.getenv("MAX_HOLD_HOURS", "48"))
RSI_EXIT = float(os.getenv("RSI_EXIT", "70"))
RSI_PERIOD = 56
TRAILING_STOP = os.getenv("TRAILING_STOP", "true").lower() == "true"
TRAILING_ATR = float(os.getenv("TRAILING_ATR", "2.0"))
TRAILING_ACTIVATION = float(os.getenv("TRAILING_ACTIVATION", "1.0"))  # ativa após N ATRs de lucro

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://crypto_admin:ZNG5z43LaSrk7FEmwu6CPtRUB2IVKdvY@crypto-postgres:5432/crypto_bot")


class PositionMonitor:
    def __init__(self):
        self.nc = None
        self.js = None
        self.kv = None
        self.exchange = ccxt.binance({
            "apiKey": BINANCE_API_KEY,
            "secret": BINANCE_API_SECRET,
            "enableRateLimit": True,
        })
        self.positions = {}  # symbol -> position data (cache local)

    async def connect_nats(self):
        self.nc = await nats.connect(NATS_URL)
        self.js = self.nc.jetstream()
        self.kv = await self.js.key_value("active_positions")
        logger.info(f"NATS conectado: {NATS_URL}")

    async def init_db(self):
        """Cria tabela trade_log se não existir."""
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trade_log (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    tier VARCHAR(30),
                    strategy VARCHAR(50),
                    direction VARCHAR(10) DEFAULT 'LONG',
                    dry_run BOOLEAN DEFAULT TRUE,
                    score FLOAT,
                    rsi FLOAT,
                    entry_price FLOAT NOT NULL,
                    quantity FLOAT NOT NULL,
                    sl_price FLOAT,
                    tp_price FLOAT,
                    exit_price FLOAT,
                    exit_reason VARCHAR(30),
                    pnl_pct FLOAT,
                    hold_hours FLOAT,
                    status VARCHAR(10) DEFAULT 'OPEN',
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
            """)
            conn.commit()
            cur.close()
            conn.close()
            logger.info("Banco de dados conectado, tabela trade_log pronta")
        except Exception as e:
            logger.error(f"Erro ao inicializar banco: {e}")

    async def log_trade_open(self, execution):
        """Insere trade no banco como OPEN."""
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO trade_log (symbol, tier, strategy, direction, dry_run,
                    score, rsi, entry_price, quantity, sl_price, tp_price, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'OPEN')
                RETURNING id;
            """, (
                execution.get("symbol"),
                execution.get("tier"),
                execution.get("strategy"),
                execution.get("direction", "LONG"),
                DRY_RUN,
                execution.get("score"),
                execution.get("rsi"),
                execution.get("entry_price"),
                execution.get("quantity"),
                execution.get("sl_price"),
                execution.get("tp_price"),
            ))
            trade_id = cur.fetchone()[0]
            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"  DB: trade #{trade_id} OPEN — {execution['symbol']}")
            return trade_id
        except Exception as e:
            logger.error(f"Erro ao logar abertura no banco: {e}")
            return None

    async def log_trade_close(self, symbol, reason_data, pos):
        """Atualiza trade no banco como CLOSED."""
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            cur.execute("""
                UPDATE trade_log
                SET exit_price = %s, exit_reason = %s, pnl_pct = %s,
                    hold_hours = %s, status = 'CLOSED', updated_at = NOW()
                WHERE id = (
                    SELECT id FROM trade_log
                    WHERE symbol = %s AND status = 'OPEN'
                    ORDER BY created_at DESC LIMIT 1
                );
            """, (
                reason_data["price"],
                reason_data["reason"],
                reason_data["pnl_pct"],
                (time.time() - pos.get("entry_time", time.time())) / 3600,
                symbol,
            ))
            updated = cur.rowcount
            conn.commit()
            cur.close()
            conn.close()
            if updated:
                logger.info(f"  DB: trade CLOSED — {symbol} ({reason_data['reason']})")
        except Exception as e:
            logger.error(f"Erro ao logar fechamento no banco: {e}")

    async def load_positions_from_kv(self):
        """Carrega posições salvas no KV store ao iniciar."""
        try:
            keys = await self.kv.keys()
            for key in keys:
                try:
                    entry = await self.kv.get(key)
                    pos = json.loads(entry.value.decode())
                    self.positions[key] = pos
                    logger.info(f"Posição carregada: {key}")
                except Exception as e:
                    logger.warning(f"Erro ao carregar posição {key}: {e}")
        except Exception as e:
            logger.error(f"Erro ao carregar KV: {e}")

    def compute_rsi(self, closes):
        import pandas as pd
        delta = np.diff(closes)
        gain = np.maximum(delta, 0)
        loss = -np.minimum(delta, 0)
        avg_gain = pd.Series(gain).rolling(RSI_PERIOD).mean().values
        avg_loss = pd.Series(loss).rolling(RSI_PERIOD).mean().values
        rsi = 100 - 100 / (1 + avg_gain / (avg_loss + 1e-10))
        return float(rsi[-1])

    async def sync_position_with_exchange(self, symbol, pos):
        """Verifica se a posição ainda existe na Binance. Se não, foi fechada externamente."""
        if DRY_RUN:
            return None  # sem sincronia em dry run
        try:
            base = symbol.split("/")[0]
            balance = self.exchange.fetch_balance()
            total_amount = balance["total"].get(base, 0)
            if total_amount <= 0:
                # Posição foi fechada externamente (OC/SL/TP executou na Binance)
                try:
                    ticker = self.exchange.fetch_ticker(symbol)
                    exit_price = ticker["last"]
                except Exception:
                    exit_price = pos.get("entry_price", 0)
                pnl_pct = (exit_price / pos["entry_price"] - 1) * 100 if pos.get("entry_price", 0) > 0 else 0
                return {"reason": "EXCHANGE_CLOSED", "price": exit_price, "pnl_pct": pnl_pct}
        except Exception as e:
            logger.error(f"Erro sync {symbol}: {e}")
        return None  # posição ainda existe

    async def check_position(self, symbol, pos):
        """Verifica se uma posição deve ser fechada."""
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            current_price = ticker["last"]
        except Exception as e:
            logger.error(f"Erro ao buscar ticker {symbol}: {e}")
            return None

        entry_price = pos["entry_price"]
        sl_price = pos.get("sl_price", entry_price * 0.98)
        tp_price = pos.get("tp_price", entry_price * 1.06)
        entry_time = pos.get("entry_time", time.time())
        hold_hours = (time.time() - entry_time) / 3600

        # 1. Check SL
        if current_price <= sl_price:
            return {"reason": "STOP_LOSS", "price": current_price, "pnl_pct": (current_price / entry_price - 1) * 100}

        # 2. Check TP
        if current_price >= tp_price:
            return {"reason": "TAKE_PROFIT", "price": current_price, "pnl_pct": (current_price / entry_price - 1) * 100}

        # 3. Check Time Exit
        if hold_hours >= MAX_HOLD_HOURS:
            return {"reason": "TIME_EXIT", "price": current_price, "pnl_pct": (current_price / entry_price - 1) * 100}

        # 4. Check RSI Reversal (se sobrecomprado, fecha mean reversion)
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, "15m", limit=200)
            closes = [c[4] for c in ohlcv]
            if len(closes) >= RSI_PERIOD + 1:
                rsi = self.compute_rsi(closes)
                if rsi >= RSI_EXIT:
                    return {"reason": "RSI_REVERSAL", "price": current_price, "rsi": rsi, "pnl_pct": (current_price / entry_price - 1) * 100}
        except Exception as e:
            logger.error(f"Erro RSI {symbol}: {e}")

        # 5. Trailing Stop
        if TRAILING_STOP and TRAILING_ACTIVATION > 0:
            try:
                ohlcv = self.exchange.fetch_ohlcv(symbol, "15m", limit=50)
                highs = [c[2] for c in ohlcv]
                lows = [c[3] for c in ohlcv]
                tr_closes = [c[4] for c in ohlcv]
                tr = np.maximum.reduce([
                    np.array(highs[1:]) - np.array(lows[1:]),
                    np.abs(np.array(highs[1:]) - np.array(tr_closes[:-1])),
                    np.abs(np.array(lows[1:]) - np.array(tr_closes[:-1])),
                ])
                atr = float(np.mean(tr[-14:])) if len(tr) >= 14 else 0.01

                profit_atr = (current_price - entry_price) / atr if atr > 0 else 0

                if profit_atr >= TRAILING_ACTIVATION:
                    new_sl = current_price - TRAILING_ATR * atr
                    old_sl = pos.get("sl_price", 0)
                    if new_sl > old_sl:
                        pos["sl_price"] = new_sl
                        pos["trailing_active"] = True
                        await self.kv.put(symbol.replace("/", "_"), json.dumps(pos).encode())
                        logger.info(f"  {symbol}: trailing stop atualizado SL={new_sl:.4f} (profit={profit_atr:.1f} ATR)")
            except Exception as e:
                logger.error(f"Erro trailing {symbol}: {e}")

        return None  # mantém posição

    async def close_position(self, symbol, pos, reason_data):
        """Fecha posição: cancela ordens restantes e publica trade.closed."""
        reason = reason_data["reason"]
        price = reason_data["price"]
        pnl_pct = reason_data["pnl_pct"]
        entry_price = pos["entry_price"]
        quantity = pos["quantity"]

        logger.info(f"FECHANDO {symbol}: {reason} @ {price:.6f} | PnL: {pnl_pct:.2f}%")

        # Cancela ordens abertas de SL/TP na Binance
        if not DRY_RUN:
            try:
                self.exchange.cancel_all_orders(symbol)
                logger.info(f"  {symbol}: ordens SL/TP canceladas")
            except Exception as e:
                logger.error(f"  {symbol}: erro ao cancelar ordens: {e}")

            # Market sell para fechar posição
            try:
                sell_order = self.exchange.create_order(symbol, "market", "sell", quantity)
                logger.info(f"  {symbol}: SELL executado {quantity} @ ~{price}")
            except Exception as e:
                logger.error(f"  {symbol}: erro ao vender: {e}")

        # Remove do KV
        key = symbol.replace("/", "_")
        try:
            await self.kv.delete(key)
        except Exception:
            pass

        if symbol in self.positions:
            del self.positions[symbol]

        # Log no banco
        await self.log_trade_close(symbol, reason_data, pos)

        # Publica trade.closed
        close_event = {
            "symbol": symbol,
            "tier": pos.get("tier", ""),
            "strategy": pos.get("strategy", ""),
            "entry_price": entry_price,
            "exit_price": price,
            "quantity": quantity,
            "exit_reason": reason,
            "pnl_pct": round(pnl_pct, 2),
            "hold_hours": round((time.time() - pos.get("entry_time", time.time())) / 3600, 2),
            "timestamp": time.time(),
        }
        payload = json.dumps(close_event).encode()
        await self.js.publish("trade.closed", payload)
        logger.info(f"  {symbol}: trade.closed publicado")

    async def process_executed_trade(self, msg):
        """Registra nova posição quando trade é executado."""
        try:
            executions = json.loads(msg.data.decode())
            for ex in executions:
                symbol = ex["symbol"]
                key = symbol.replace("/", "_")

                pos = {
                    "symbol": symbol,
                    "tier": ex.get("tier", ""),
                    "strategy": ex.get("strategy", ""),
                    "quantity": ex["quantity"],
                    "entry_price": ex["entry_price"],
                    "sl_price": ex.get("sl_price", 0),
                    "tp_price": ex.get("tp_price", 0),
                    "entry_time": time.time(),
                    "trailing_active": False,
                }

                self.positions[key] = pos
                await self.kv.put(key, json.dumps(pos).encode())
                await self.log_trade_open(ex)
                logger.info(f"POSIÇÃO ABERTA: {symbol} qty={ex['quantity']} entry={ex['entry_price']}")

            await msg.ack()
        except Exception as e:
            logger.error(f"Erro ao processar trade executado: {e}")

    async def monitor_loop(self):
        """Loop principal: verifica todas as posições a cada MONITOR_INTERVAL segundos."""
        logger.info(f"fb-position-monitor online (max_hold={MAX_HOLD_HOURS}h, rsi_exit={RSI_EXIT}, trailing={TRAILING_STOP})")
        while True:
            if self.nc.is_closed:
                await self.connect_nats()
                await self.load_positions_from_kv()

            positions_snapshot = list(self.positions.items())
            for key, pos in positions_snapshot:
                symbol = pos["symbol"]
                # 0. Sync: verifica se posição ainda existe na Binance (fechada por OCO?)
                exchange_result = await self.sync_position_with_exchange(symbol, pos)
                if exchange_result:
                    await self.close_position(symbol, pos, exchange_result)
                    continue
                # 1-5. Checks normais (TP/SL/RSI/time/trailing)
                result = await self.check_position(symbol, pos)
                if result:
                    await self.close_position(symbol, pos, result)

            await asyncio.sleep(MONITOR_INTERVAL)

    async def run(self):
        await self.connect_nats()
        await self.init_db()
        await self.load_positions_from_kv()

        # Subscreve a trade.executed para registrar novas posições
        await self.js.subscribe("trade.executed", durable="POSITION_MONITOR_WORKER",
                                 cb=self.process_executed_trade, manual_ack=True,
                                 config=ConsumerConfig(ack_wait=30))

        await self.monitor_loop()


if __name__ == "__main__":
    monitor = PositionMonitor()
    asyncio.run(monitor.run())
