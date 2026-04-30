import time
import logging
import os
import json
import redis
import ccxt

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("position-management")

# Configurações via Ambiente
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

class PositionManagementService:
    def __init__(self):
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.exchange = ccxt.binance()

    def monitor_positions(self):
        """Loop principal de monitoramento de posições ativas."""
        while True:
            # Busca todas as posições ativas no Redis
            positions = self.r.hgetall("positions:active")
            
            if not positions:
                logger.info("Nenhuma posição ativa para monitorar.")
                time.sleep(30)
                continue

            for symbol, data in positions.items():
                trade = json.loads(data)
                
                # Busca preço atual
                try:
                    ticker = self.exchange.fetch_ticker(symbol)
                    current_price = ticker['last']
                    
                    logger.info(f"Monitorando {symbol}: Preço Atual {current_price} | SL: {trade['stop_loss']} | TP: {trade['take_profit']}")
                    
                    # Lógica de Fechamento (Simplificada)
                    if current_price <= trade['stop_loss']:
                        self.close_position(symbol, trade, "STOP_LOSS", current_price)
                    elif current_price >= trade['take_profit']:
                        self.close_position(symbol, trade, "TAKE_PROFIT", current_price)
                    
                except Exception as e:
                    logger.error(f"Erro ao monitorar {symbol}: {e}")

            time.sleep(10) # Frequência de monitoramento

    def close_position(self, symbol, trade, reason, price):
        """Fecha a posição e notifica o sistema."""
        logger.info(f"FECHANDO POSIÇÃO: {symbol} por {reason} ao preço {price}")
        
        close_event = {
            **trade,
            "exit_price": price,
            "exit_reason": reason,
            "exit_at": time.time(),
            "pnl": (price - trade['entry_price']) / trade['entry_price']
        }
        
        # Remove do Redis de ativos e publica evento de encerramento
        self.r.hdel("positions:active", symbol)
        self.r.publish("events:trade_closed", json.dumps(close_event))

if __name__ == "__main__":
    service = PositionManagementService()
    service.monitor_positions()
