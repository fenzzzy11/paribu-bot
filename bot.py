import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests


TICKER_URL = "https://www.paribu.com/ticker"
STATE_FILE = Path("sent_signals.json")
HTML_FILE = Path("index.html")
TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}/sendMessage"
MAX_HISTORY_POINTS = 100
MIN_VOLUME_TRY = 1_000_000.0


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("paribu-bot")


def utc_now_iso() -> str:
    try:
        return datetime.now(timezone.utc).isoformat()
    except Exception:
        return datetime.utcnow().isoformat() + "Z"


def load_state() -> Dict[str, Any]:
    default_state = {
        "signals": {},
        "price_history": {},
        "last_run_utc": "",
    }
    try:
        if not STATE_FILE.exists():
            logger.info("State file not found, using default state.")
            return default_state
        with STATE_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            logger.warning("State file invalid format, resetting state.")
            return default_state
        data.setdefault("signals", {})
        data.setdefault("price_history", {})
        data.setdefault("last_run_utc", "")
        logger.info("State loaded successfully.")
        return data
    except Exception as exc:
        logger.exception("State load error: %s", exc)
        return default_state


def save_state(state: Dict[str, Any], run_ts: str = "") -> None:
    try:
        state["last_run_utc"] = run_ts or utc_now_iso()
        with STATE_FILE.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        logger.info("State saved successfully.")
    except Exception as exc:
        logger.exception("State save error: %s", exc)


def fetch_ticker_data(max_retries: int = 3, wait_seconds: int = 5) -> Dict[str, Any]:
    try:
        for attempt in range(1, max_retries + 1):
            try:
                logger.info("Fetching ticker data (attempt %s/%s).", attempt, max_retries)
                response = requests.get(TICKER_URL, timeout=20)
                response.raise_for_status()
                data = response.json()
                if not isinstance(data, dict):
                    raise ValueError("Ticker response is not a JSON object.")
                if not data:
                    raise ValueError("Ticker response is empty.")
                logger.info("Ticker data fetched successfully.")
                return data
            except Exception as exc:
                logger.warning("Ticker fetch failed on attempt %s: %s", attempt, exc)
                if attempt < max_retries:
                    logger.info("Waiting %s seconds before retry.", wait_seconds)
                    time.sleep(wait_seconds)
        logger.error("All ticker fetch attempts failed.")
        return {}
    except Exception as exc:
        logger.exception("Ticker fetch error: %s", exc)
        return {}


def parse_float(raw_value: Any, default: float = 0.0) -> float:
    try:
        if raw_value is None:
            return default
        if isinstance(raw_value, str):
            cleaned = raw_value.replace("%", "").replace(",", ".").strip()
            if cleaned == "":
                return default
            return float(cleaned)
        return float(raw_value)
    except Exception:
        return default


def parse_market_data(ticker_data: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    market_data: Dict[str, Dict[str, float]] = {}
    try:
        if not ticker_data:
            logger.warning("Ticker data is empty; parse skipped.")
            return market_data
        for pair, payload in ticker_data.items():
            try:
                if not isinstance(payload, dict):
                    continue
                if not pair.endswith("_TL"):
                    continue

                last_price = parse_float(payload.get("last"), default=0.0)
                if last_price <= 0:
                    continue

                change_24h = parse_float(payload.get("daily"), default=0.0)
                volume = parse_float(
                    payload.get("volume")
                    or payload.get("vol")
                    or payload.get("volume_24h")
                    or payload.get("baseVolume"),
                    default=0.0,
                )
                market_data[pair] = {
                    "price": last_price,
                    "change_24h": change_24h,
                    "volume": volume,
                }
            except Exception:
                continue
        logger.info("Parsed market data for %s pairs.", len(market_data))
        return market_data
    except Exception as exc:
        logger.exception("Market parse error: %s", exc)
        return market_data


def update_price_history(state: Dict[str, Any], market_data: Dict[str, Dict[str, float]], max_points: int = MAX_HISTORY_POINTS) -> None:
    try:
        history = state.setdefault("price_history", {})
        for pair, data in market_data.items():
            try:
                price = parse_float(data.get("price"), default=0.0)
                if price <= 0:
                    continue
                coin_hist = history.get(pair, [])
                if not isinstance(coin_hist, list):
                    coin_hist = []
                coin_hist.append(price)
                history[pair] = coin_hist[-max_points:]
            except Exception:
                continue
        logger.info("Price history updated for %s pairs.", len(market_data))
    except Exception as exc:
        logger.exception("Price history update error: %s", exc)


def calculate_rsi(prices: List[float], period: int = 14) -> float:
    try:
        if len(prices) <= period:
            return 0.0
        series = pd.Series(prices, dtype="float64")
        delta = series.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(period).mean().iloc[-1]
        avg_loss = loss.rolling(period).mean().iloc[-1]
        if pd.isna(avg_gain) or pd.isna(avg_loss):
            return 0.0
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return float(100 - (100 / (1 + rs)))
    except Exception as exc:
        logger.exception("RSI calculation error: %s", exc)
        return 0.0


def find_bullish_signals(state: Dict[str, Any], market_data: Dict[str, Dict[str, float]]) -> List[Tuple[str, float, float, float, float]]:
    bullish: List[Tuple[str, float, float, float, float]] = []
    history = state.get("price_history", {})
    try:
        volumes = [
            parse_float(data.get("volume"), default=0.0)
            for data in market_data.values()
            if parse_float(data.get("volume"), default=0.0) > 0
        ]
        dynamic_threshold = float(pd.Series(volumes).quantile(0.6)) if volumes else MIN_VOLUME_TRY
        volume_threshold = max(MIN_VOLUME_TRY, dynamic_threshold)
        logger.info("Volume threshold set to %.2f", volume_threshold)

        for pair, data in market_data.items():
            try:
                current = parse_float(data.get("price"), default=0.0)
                change_24h = parse_float(data.get("change_24h"), default=0.0)
                volume = parse_float(data.get("volume"), default=0.0)
                coin_history = history.get(pair, [])

                if current <= 0 or not isinstance(coin_history, list):
                    continue
                if len(coin_history) < 21:
                    continue
                if change_24h <= 0:
                    continue
                if volume < volume_threshold:
                    continue

                series = pd.Series(coin_history, dtype="float64")
                ma5_now = series.rolling(5).mean().iloc[-1]
                ma20_now = series.rolling(20).mean().iloc[-1]
                ma5_prev = series.rolling(5).mean().iloc[-2]
                ma20_prev = series.rolling(20).mean().iloc[-2]
                if pd.isna(ma5_now) or pd.isna(ma20_now) or pd.isna(ma5_prev) or pd.isna(ma20_prev):
                    continue

                golden_cross = ma5_prev <= ma20_prev and ma5_now > ma20_now
                if not golden_cross:
                    continue

                rsi = calculate_rsi(coin_history, period=14)
                if rsi > 70:
                    logger.info("Skipping %s due to RSI %.2f > 70.", pair, rsi)
                    continue

                bullish.append((pair, current, change_24h, volume, rsi))
            except Exception:
                continue
    except Exception as exc:
        logger.exception("Signal scan error: %s", exc)
    logger.info("Bullish signals found: %s", len(bullish))
    return bullish


def send_telegram_message(message: str, token: str, chat_id: str) -> bool:
    try:
        if not token or not chat_id:
            logger.warning("Telegram credentials missing; message not sent.")
            return False
        url = TELEGRAM_API_BASE.format(token=token)
        payload = {"chat_id": chat_id, "text": message}
        response = requests.post(url, json=payload, timeout=20)
        response.raise_for_status()
        logger.info("Telegram message sent successfully.")
        return True
    except Exception as exc:
        logger.exception("Telegram send error: %s", exc)
        return False


def process_signals(state: Dict[str, Any], signals_to_send: List[Tuple[str, float, float, float, float]], token: str, chat_id: str) -> None:
    try:
        signals = state.setdefault("signals", {})
        for pair, current_price, change_24h, volume, rsi in signals_to_send:
            try:
                target = current_price * 1.03
                stop_loss = current_price * 0.98
                paribu_link = f"https://www.paribu.com/markets/{pair}"
                msg = (
                    f"🚀 Sinyal Bulundu: {pair}\n"
                    f"💰 Fiyat: {current_price:.6f} TL\n"
                    f"📈 24s Değişim: %{change_24h:.2f}\n"
                    f"📊 Hacim: {volume:,.2f}\n"
                    f"🎯 Hedef 1: {target:.6f} TL\n"
                    f"🛑 Stop Loss: {stop_loss:.6f} TL\n"
                    f"🔗 {paribu_link}"
                )
                sent = send_telegram_message(msg, token, chat_id)
                if sent:
                    signals[pair] = {
                        "signal_price": current_price,
                        "change_24h": change_24h,
                        "volume": volume,
                        "rsi": rsi,
                        "last_signal_utc": utc_now_iso(),
                    }
            except Exception as exc:
                logger.exception("Signal process error for %s: %s", pair, exc)
    except Exception as exc:
        logger.exception("Signal processing error: %s", exc)


def process_drop_alerts(state: Dict[str, Any], prices: Dict[str, float], token: str, chat_id: str) -> None:
    try:
        signals = state.setdefault("signals", {})
        to_remove: List[str] = []
        for pair, entry in signals.items():
            try:
                if not isinstance(entry, dict):
                    continue
                signal_price = parse_float(entry.get("signal_price"), default=0.0)
                if signal_price <= 0:
                    continue
                current = prices.get(pair)
                if current is None:
                    continue
                drop_threshold = signal_price * 0.98
                if current <= drop_threshold:
                    msg = f"⚠️ {pair} için düşüş uyarısı! Fiyat {current:.6f}'na geriledi."
                    _ = send_telegram_message(msg, token, chat_id)
                    to_remove.append(pair)
            except Exception as exc:
                logger.exception("Drop alert error for %s: %s", pair, exc)
        for pair in to_remove:
            signals.pop(pair, None)
        if to_remove:
            logger.info("Removed %s signals after drop alerts.", len(to_remove))
    except Exception as exc:
        logger.exception("Drop alert processing error: %s", exc)


def build_html_rows(market_data: Dict[str, Dict[str, float]], bullish_pairs: List[str], rsi_map: Dict[str, float]) -> str:
    try:
        rows: List[str] = []
        for pair in sorted(market_data.keys()):
            try:
                price = parse_float(market_data[pair].get("price"), default=0.0)
                change_24h = parse_float(market_data[pair].get("change_24h"), default=0.0)
                volume = parse_float(market_data[pair].get("volume"), default=0.0)
                rsi_value = rsi_map.get(pair, 0.0)
                trend = "Yukselis" if pair in bullish_pairs else "Normal"
                badge_class = "up" if trend == "Yukselis" else "flat"
                rows.append(
                    "<tr>"
                    f"<td>{pair}</td>"
                    f"<td>{price:.6f} TL</td>"
                    f"<td>%{change_24h:.2f}</td>"
                    f"<td>{volume:,.2f}</td>"
                    f"<td>{rsi_value:.2f}</td>"
                    f"<td><span class='badge {badge_class}'>{trend}</span></td>"
                    "</tr>"
                )
            except Exception:
                continue
        return "\n".join(rows)
    except Exception as exc:
        logger.exception("HTML row build error: %s", exc)
        return ""


def write_html(market_data: Dict[str, Dict[str, float]], bullish: List[Tuple[str, float, float, float, float]], state: Dict[str, Any]) -> None:
    try:
        bullish_pairs = [x[0] for x in bullish]
        rsi_map = {pair: rsi for pair, _, _, _, rsi in bullish}
        rows = build_html_rows(market_data, bullish_pairs, rsi_map)
        updated = state.get("last_run_utc") or utc_now_iso()
        html = f"""<!doctype html>
<html lang="tr">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Paribu Bot Raporu</title>
  <style>
    :root {{
      --bg: #0f172a;
      --panel: #111827;
      --text: #e5e7eb;
      --muted: #9ca3af;
      --line: #1f2937;
      --up: #10b981;
      --flat: #64748b;
    }}
    body {{
      margin: 0;
      font-family: Arial, sans-serif;
      background: linear-gradient(180deg, #0b1220 0%, #0f172a 100%);
      color: var(--text);
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 20px;
    }}
    .card {{
      width: min(1100px, 96vw);
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      box-shadow: 0 16px 50px rgba(0,0,0,0.35);
      overflow: hidden;
    }}
    .header {{
      padding: 18px 22px;
      border-bottom: 1px solid var(--line);
    }}
    h1 {{
      margin: 0;
      font-size: 22px;
    }}
    .meta {{
      margin-top: 8px;
      color: var(--muted);
      font-size: 14px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
    }}
    th, td {{
      padding: 12px 16px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      font-size: 14px;
    }}
    th {{
      color: #cbd5e1;
      font-weight: 600;
    }}
    .badge {{
      display: inline-block;
      padding: 3px 9px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
    }}
    .up {{ background: rgba(16,185,129,0.15); color: #34d399; }}
    .flat {{ background: rgba(100,116,139,0.2); color: #cbd5e1; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="header">
      <h1>Paribu Coin Analiz Raporu</h1>
      <div class="meta">Son calisma (UTC): {updated}</div>
    </div>
    <table>
      <thead>
        <tr>
          <th>Coin Cifti</th>
          <th>Fiyat</th>
          <th>24s Degisim</th>
          <th>Hacim</th>
          <th>RSI</th>
          <th>Durum</th>
        </tr>
      </thead>
      <tbody>
        {rows if rows else "<tr><td colspan='6'>Veri yok</td></tr>"}
      </tbody>
    </table>
  </div>
</body>
</html>
"""
        HTML_FILE.write_text(html, encoding="utf-8")
        logger.info("HTML report updated.")
    except Exception as exc:
        logger.exception("HTML write error: %s", exc)


def main() -> None:
    try:
        run_ts = utc_now_iso()
        logger.info("Bot run started at %s", run_ts)
        token = os.environ.get("TELEGRAM_TOKEN", "").strip()
        chat_id = os.environ.get("CHAT_ID", "").strip()

        state = load_state()
        ticker_data = fetch_ticker_data()
        market_data = parse_market_data(ticker_data)
        if not market_data:
            logger.error("No valid market data found from ticker.")
            write_html({}, [], state)
            save_state(state, run_ts=run_ts)
            return

        update_price_history(state, market_data)
        bullish = find_bullish_signals(state, market_data)
        top_signals = sorted(bullish, key=lambda item: item[2], reverse=True)[:3]
        process_signals(state, top_signals, token, chat_id)

        prices = {pair: parse_float(data.get("price"), default=0.0) for pair, data in market_data.items()}
        process_drop_alerts(state, prices, token, chat_id)

        state["last_run_utc"] = run_ts
        write_html(market_data, bullish, state)
        save_state(state, run_ts=run_ts)
        logger.info(
            "Run completed. Coins: %s, Bullish signals: %s, Sent signals: %s",
            len(market_data),
            len(bullish),
            len(top_signals),
        )
    except Exception as exc:
        logger.exception("Fatal runtime error: %s", exc)
        try:
            fallback_state = load_state()
            write_html({}, [], fallback_state)
            save_state(fallback_state, run_ts=utc_now_iso())
        except Exception as inner_exc:
            logger.exception("Fatal fallback error: %s", inner_exc)


if __name__ == "__main__":
    main()
