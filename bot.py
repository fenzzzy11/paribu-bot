import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests


TICKER_URL = "https://www.paribu.com/ticker"
STATE_FILE = Path("sent_signals.json")
HTML_FILE = Path("index.html")
TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}/sendMessage"


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
            return default_state
        with STATE_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return default_state
        data.setdefault("signals", {})
        data.setdefault("price_history", {})
        data.setdefault("last_run_utc", "")
        return data
    except Exception as exc:
        print(f"State load error: {exc}")
        return default_state


def save_state(state: Dict[str, Any], run_ts: str = "") -> None:
    try:
        state["last_run_utc"] = run_ts or utc_now_iso()
        with STATE_FILE.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        print(f"State save error: {exc}")


def fetch_ticker_data() -> Dict[str, Any]:
    try:
        response = requests.get(TICKER_URL, timeout=20)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError("Ticker response is not a JSON object.")
        return data
    except Exception as exc:
        print(f"Ticker fetch error: {exc}")
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
        for pair, payload in ticker_data.items():
            try:
                if not isinstance(payload, dict):
                    continue
                if not pair.endswith("_TL"):
                    continue
                raw_last = payload.get("last")
                if raw_last is None:
                    continue
                last_price = parse_float(raw_last, default=0.0)
                if last_price <= 0:
                    continue
                change_24h = parse_float(payload.get("daily"), default=0.0)
                market_data[pair] = {
                    "price": last_price,
                    "change_24h": change_24h,
                }
            except Exception:
                continue
        return market_data
    except Exception as exc:
        print(f"Market parse error: {exc}")
        return market_data


def find_bullish_signals(market_data: Dict[str, Dict[str, float]]) -> List[Tuple[str, float, float]]:
    bullish: List[Tuple[str, float, float]] = []
    try:
        for pair, data in market_data.items():
            try:
                current = parse_float(data.get("price"), default=0.0)
                change_24h = parse_float(data.get("change_24h"), default=0.0)
                if current <= 0:
                    continue
                if change_24h > 0:
                    bullish.append((pair, current, change_24h))
            except Exception:
                continue
    except Exception as exc:
        print(f"Signal scan error: {exc}")
    return bullish


def send_telegram_message(message: str, token: str, chat_id: str) -> bool:
    try:
        if not token or not chat_id:
            print("Telegram credentials missing; message not sent.")
            return False
        url = TELEGRAM_API_BASE.format(token=token)
        payload = {"chat_id": chat_id, "text": message}
        response = requests.post(url, json=payload, timeout=20)
        response.raise_for_status()
        return True
    except Exception as exc:
        print(f"Telegram send error: {exc}")
        return False


def process_signals(state: Dict[str, Any], bullish: List[Tuple[str, float, float]], token: str, chat_id: str) -> None:
    try:
        signals = state.setdefault("signals", {})
        for pair, current_price, change_24h in bullish:
            try:
                target = current_price * 1.02
                msg = (
                    f"🚀 {pair} Sinyali! Güncel Fiyat: {current_price:.6f} TL. "
                    f"Hedef Fiyat (+%2): {target:.6f} TL. "
                    f"24s Degisim: %{change_24h:.2f}"
                )
                sent = send_telegram_message(msg, token, chat_id)
                if sent:
                    signals[pair] = {
                        "signal_price": current_price,
                        "change_24h": change_24h,
                        "last_signal_utc": utc_now_iso(),
                    }
            except Exception as exc:
                print(f"Signal process error for {pair}: {exc}")
    except Exception as exc:
        print(f"Signal processing error: {exc}")


def process_drop_alerts(state: Dict[str, Any], prices: Dict[str, float], token: str, chat_id: str) -> None:
    try:
        signals = state.setdefault("signals", {})
        to_remove: List[str] = []
        for pair, entry in signals.items():
            try:
                if not isinstance(entry, dict):
                    continue
                signal_price = float(entry.get("signal_price", 0))
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
                print(f"Drop alert error for {pair}: {exc}")
        for pair in to_remove:
            signals.pop(pair, None)
    except Exception as exc:
        print(f"Drop alert processing error: {exc}")


def build_html_rows(market_data: Dict[str, Dict[str, float]], bullish_pairs: List[str]) -> str:
    try:
        rows: List[str] = []
        for pair in sorted(market_data.keys()):
            try:
                price = parse_float(market_data[pair].get("price"), default=0.0)
                change_24h = parse_float(market_data[pair].get("change_24h"), default=0.0)
                trend = "Yukselis" if pair in bullish_pairs else "Normal"
                badge_class = "up" if trend == "Yukselis" else "flat"
                rows.append(
                    "<tr>"
                    f"<td>{pair}</td>"
                    f"<td>{price:.6f} TL</td>"
                    f"<td>%{change_24h:.2f}</td>"
                    f"<td><span class='badge {badge_class}'>{trend}</span></td>"
                    "</tr>"
                )
            except Exception:
                continue
        return "\n".join(rows)
    except Exception as exc:
        print(f"HTML row build error: {exc}")
        return ""


def write_html(market_data: Dict[str, Dict[str, float]], bullish: List[Tuple[str, float, float]], state: Dict[str, Any]) -> None:
    try:
        bullish_pairs = [x[0] for x in bullish]
        rows = build_html_rows(market_data, bullish_pairs)
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
      width: min(1000px, 96vw);
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
          <th>Durum</th>
        </tr>
      </thead>
      <tbody>
        {rows if rows else "<tr><td colspan='4'>Veri yok</td></tr>"}
      </tbody>
    </table>
  </div>
</body>
</html>
"""
        HTML_FILE.write_text(html, encoding="utf-8")
    except Exception as exc:
        print(f"HTML write error: {exc}")


def main() -> None:
    try:
        run_ts = utc_now_iso()
        token = os.environ.get("TELEGRAM_TOKEN", "").strip()
        chat_id = os.environ.get("CHAT_ID", "").strip()

        state = load_state()
        ticker_data = fetch_ticker_data()
        market_data = parse_market_data(ticker_data)
        if not market_data:
            print("No prices found from ticker.")
            write_html({}, [], state)
            save_state(state, run_ts=run_ts)
            return

        bullish = find_bullish_signals(market_data)
        top_gainers = sorted(
            [
                (pair, parse_float(data.get("price"), default=0.0), parse_float(data.get("change_24h"), default=0.0))
                for pair, data in market_data.items()
            ],
            key=lambda item: item[2],
            reverse=True,
        )[:3]
        process_signals(state, top_gainers, token, chat_id)
        prices = {pair: parse_float(data.get("price"), default=0.0) for pair, data in market_data.items()}
        process_drop_alerts(state, prices, token, chat_id)
        state["last_run_utc"] = run_ts
        write_html(market_data, bullish, state)
        save_state(state, run_ts=run_ts)
        print(f"Run completed. Coins: {len(market_data)}, Bullish signals: {len(bullish)}, Top gainers messaged: {len(top_gainers)}")
    except Exception as exc:
        print(f"Fatal runtime error: {exc}")
        try:
            fallback_state = load_state()
            write_html({}, [], fallback_state)
            save_state(fallback_state, run_ts=utc_now_iso())
        except Exception as inner_exc:
            print(f"Fatal fallback error: {inner_exc}")


if __name__ == "__main__":
    main()
