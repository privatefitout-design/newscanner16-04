"""
SCREENER LABS — PRE-BREAKOUT SCANNER v2
16.04.2026

ЛОГИКА:
  Фаза 1: Цена в базе 20-180 дней, EMA20/50 горизонтальны и сжаты
  Фаза 2: OI растёт плавно 45-65°, цена спит
  Фаза 3: NATR просыпается

SCORING:
  Фаза 1: макс 50 (длина базы + амплитуда + EMA)
  Фаза 2: макс 40 (угол OI + плавность + цена тихая)
  Фаза 3: макс 10 (NATR пробуждение)
  Итого:  макс 100

BADGES:
  90-100: EXCEPTIONAL
  75-89:  STRONG+
  60-74:  STRONG
  40-59:  AVERAGE
  <40:    без метки
"""

import time, logging, requests, pandas as pd, numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from apscheduler.schedulers.blocking import BlockingScheduler

# ── НАСТРОЙКИ ─────────────────────────────────────────────────
TELEGRAM_TOKEN = "8731868942:AAEKTM-hbrskq52V3wFtoKfUEr2Hn5-mrHQ"
CHAT_ID        = "181943757"

MIN_SCORE      = 40     # минимальный score для алерта
TOP_RESULTS    = 7
SCAN_HOURS     = 0.5
WORKERS        = 4

BASE_DAYS_MIN  = 20     # минимум дней в базе — ЖЁСТКО
BASE_DAYS_MAX  = 180
BASE_RANGE_MAX = 35     # максимальный диапазон базы %
OI_MIN         = 3      # минимальный рост OI 12ч %
PRICE_MAX      = 15     # максимальное движение цены за 6ч %

# ── ЛОГИРОВАНИЕ ───────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ── BINANCE API ───────────────────────────────────────────────
BASE = "https://fapi.binance.com"

def api(url, params=None):
    try:
        r = requests.get(url, params=params, timeout=10)
        return r.json() if r.status_code == 200 else None
    except:
        return None

def get_symbols():
    data = api(f"{BASE}/fapi/v1/exchangeInfo")
    if not data: return []
    return [s['symbol'] for s in data['symbols']
            if s['quoteAsset']=='USDT' and s['status']=='TRADING'
            and s['contractType']=='PERPETUAL']

def klines(symbol, interval, limit):
    data = api(f"{BASE}/fapi/v1/klines",
               {"symbol": symbol, "interval": interval, "limit": limit})
    if not data or len(data) < 5: return None
    df = pd.DataFrame(data, columns=[
        'open_time','open','high','low','close','volume',
        'close_time','quote_vol','trades','taker_buy_base','taker_buy_quote','ignore'])
    for c in ['open','high','low','close','volume']:
        df[c] = df[c].astype(float)
    return df

def oi_hist(symbol, period, limit):
    data = api(f"{BASE}/futures/data/openInterestHist",
               {"symbol": symbol, "period": period, "limit": limit})
    if not data or not isinstance(data, list): return None
    df = pd.DataFrame(data)
    df['oi'] = df['sumOpenInterest'].astype(float)
    return df

def get_prefilter():
    try:
        data = api(f"{BASE}/fapi/v1/ticker/24hr")
        if not data: return None
        return {t['symbol']: {
            'pct': abs(float(t.get('priceChangePercent', 99))),
            'vol': float(t.get('quoteVolume', 0))
        } for t in data if t['symbol'].endswith('USDT')}
    except:
        return None

# ── УТИЛИТЫ ───────────────────────────────────────────────────
def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def calc_natr(df, n=14):
    h, l, c = df['high'], df['low'], df['close']
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    return (tr.rolling(n).mean() / c * 100).round(3)

def oi_angle(series):
    try:
        y = series.values.astype(float)
        if len(y) < 3: return 0
        x = np.arange(len(y))
        mn, mx = y.min(), y.max()
        yn = (y-mn)/(mx-mn) if mx != mn else np.zeros_like(y)
        slope, _ = np.polyfit(x, yn, 1)
        return max(0, min(90, round(np.degrees(np.arctan(slope * len(y))), 1)))
    except:
        return 0

# ── ГЛАВНАЯ ФУНКЦИЯ ───────────────────────────────────────────
def scan_symbol(sym):
    try:
        k1d  = klines(sym, "1d", 200)
        k1h  = klines(sym, "1h", 50)
        oi1h = oi_hist(sym, "1h", 50)
        oi1d = oi_hist(sym, "1d", 60)

        if k1d is None or k1h is None or oi1h is None: return 0, {}
        if len(k1d) < 30 or len(k1h) < 12 or len(oi1h) < 12: return 0, {}

        score = 0
        d = {"symbol": sym}
        price = k1d['close'].iloc[-1]
        d['price'] = round(price, 8)

        # ══════════════════════════════════════════════
        # ФАЗА 1 — БАЗА (макс 50)
        # ══════════════════════════════════════════════

        # Берём фиксированное окно — от MAX до MIN, первое что проходит
        base_window = None
        base_range  = None
        for bw in [180, 120, 90, 60, 45, 30, BASE_DAYS_MIN]:
            if len(k1d) < bw + 3: continue
            b  = k1d.iloc[-bw:-3]
            bh = b['high'].max()
            bl = b['low'].min()
            br = round((bh - bl) / bl * 100, 1) if bl > 0 else 999
            if br <= BASE_RANGE_MAX:
                base_window = bw
                base_range  = br
                break

        if base_window is None: return 0, {}

        base     = k1d.iloc[-base_window:-3]
        base_hi  = base['high'].max()
        base_lo  = base['low'].min()
        d['base_days']  = base_window
        d['base_range'] = base_range

        # Цена не должна быть ниже 80% от хая базы (даунтренд после памп)
        if price < base_hi * 0.80: return 0, {}

        # EMA20 и EMA50 на дневном
        e20 = ema(k1d['close'], 20)
        e50 = ema(k1d['close'], 50)
        if len(e20) < 10: return 0, {}

        # EMA20 не должна падать (даунтренд)
        if e20.iloc[-1] < e20.iloc[-5]: return 0, {}

        # EMA gap — сжатие
        ema_gap = abs(e20.iloc[-1] - e50.iloc[-1]) / e50.iloc[-1] * 100
        d['ema_gap'] = round(ema_gap, 2)

        # Scoring Фазы 1
        # Длина базы
        if base_window >= 90:   s1_len = 35
        elif base_window >= 60: s1_len = 28
        elif base_window >= 45: s1_len = 22
        elif base_window >= 30: s1_len = 16
        else:                   s1_len = 10  # 20-29 дней

        # Амплитуда базы
        if base_range < 5:    s1_amp = 10
        elif base_range < 10: s1_amp = 7
        elif base_range < 20: s1_amp = 4
        elif base_range < 30: s1_amp = 1
        else:                 s1_amp = 0

        # EMA сжатие бонус
        s1_ema = 5 if ema_gap < 3 else (3 if ema_gap < 6 else 0)

        phase1 = min(50, s1_len + s1_amp + s1_ema)
        score += phase1
        d['phase1'] = phase1

        # ══════════════════════════════════════════════
        # ФАЗА 2 — OI НАКОПЛЕНИЕ (макс 40)
        # ══════════════════════════════════════════════

        oi_now = oi1h['oi'].iloc[-1]

        def oi_chg(n):
            if len(oi1h) < n: return 0
            v = oi1h['oi'].iloc[-n]
            return round((oi_now - v) / v * 100, 1) if v > 0 else 0

        oi3  = oi_chg(3)
        oi6  = oi_chg(6)
        oi12 = oi_chg(12)
        oi_best = max(oi3, oi6, oi12)

        d['oi3']  = oi3
        d['oi6']  = oi6
        d['oi12'] = oi12

        if oi_best < OI_MIN: return 0, {}

        # OI должен расти прямо сейчас
        if oi1h['oi'].iloc[-1] < oi1h['oi'].iloc[-3] * 1.002: return 0, {}
        # Две свечи подряд OI падает — поздно
        if oi1h['oi'].iloc[-1] < oi1h['oi'].iloc[-2] and \
           oi1h['oi'].iloc[-2] < oi1h['oi'].iloc[-3]: return 0, {}

        # Угол OI
        ang6  = oi_angle(oi1h['oi'].iloc[-6:])
        ang12 = oi_angle(oi1h['oi'].iloc[-12:])
        best_ang = max(ang6, ang12)
        d['oi_angle'] = best_ang

        # Спайк — отсев
        if best_ang > 78: return 0, {}

        # Scoring угла
        if 42 <= best_ang <= 62:   s2_ang = 22
        elif 30 <= best_ang < 42:  s2_ang = 14
        elif 62 < best_ang <= 72:  s2_ang = 10
        else:                      s2_ang = 4

        # Плавность OI
        oi_ch = oi1h['oi'].iloc[-12:].pct_change().dropna()
        smooth = round(oi_ch.std() * 100, 2)
        d['oi_smooth'] = smooth
        s2_smooth = 8 if smooth < 3 else (5 if smooth < 6 else 2)

        # Цена тихая за 6ч
        p6 = k1h['close'].iloc[-6]
        pchg = abs(round((price - p6) / p6 * 100, 2)) if p6 > 0 else 99
        d['price_chg'] = pchg

        if pchg > PRICE_MAX: return 0, {}
        s2_price = 10 if pchg < 1.5 else (7 if pchg < 3 else (4 if pchg < 5 else 1))

        phase2 = min(40, s2_ang + s2_smooth + s2_price)
        score += phase2
        d['phase2'] = phase2

        # ══════════════════════════════════════════════
        # ФАЗА 3 — NATR ПРОБУЖДЕНИЕ (макс 10)
        # ══════════════════════════════════════════════

        natr = calc_natr(k1h, n=7).dropna()
        if len(natr) >= 12:
            nb = natr.iloc[-12:-3].mean()
            nn = natr.iloc[-3:].mean()
            awaken = round(nn / nb, 2) if nb > 0 else 1
            d['natr_awaken'] = awaken
            if awaken > 4: return 0, {}  # уже памп
            s3 = 10 if awaken > 1.8 else (7 if awaken > 1.3 else (3 if awaken > 1.0 else 0))
        else:
            s3 = 0
            d['natr_awaken'] = None

        phase3 = min(10, s3)
        score += phase3
        d['phase3'] = phase3

        score = min(100, score)
        d['score'] = score
        return score, d

    except Exception as e:
        log.debug(f"Err {sym}: {e}")
        return 0, {}

# ── TELEGRAM ──────────────────────────────────────────────────
def tg_send(text):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10)
    except Exception as e:
        log.error(f"TG: {e}")

def fmt_signal(r):
    s  = r['score']
    p1 = r.get('phase1', '?')
    p2 = r.get('phase2', '?')
    p3 = r.get('phase3', '?')

    if s >= 90:   badge = " 🔥 EXCEPTIONAL"
    elif s >= 75: badge = " ⚡ STRONG+"
    elif s >= 60: badge = " STRONG"
    elif s >= 40: badge = " AVERAGE"
    else:         badge = ""

    return "\n".join([
        f"🟢 *LONG*{badge}",
        f"{'─'*24}",
        f"*{r['symbol']}*  Score: `{s}` · Ф1:`{p1}` Ф2:`{p2}` Ф3:`{p3}`",
        f"База: `{r.get('base_days','?')}д` · диапазон `{r.get('base_range','?')}%`",
        f"OI угол: `{r.get('oi_angle','?')}°`",
        f"OI: `+{r.get('oi3','?')}%` (3ч) · `+{r.get('oi6','?')}%` (6ч) · `+{r.get('oi12','?')}%` (12ч)",
        f"Цена: `±{r.get('price_chg','?')}%`",
        ""
    ])

def fmt_alert(results):
    msg = f"_{datetime.now().strftime('%d.%m.%Y %H:%M')}_\n\n"
    for r in results:
        msg += fmt_signal(r)
    return msg

# ── СКАН ──────────────────────────────────────────────────────
def run_scan():
    log.info("="*55)
    log.info(f"SCREENER LABS  {datetime.now().strftime('%d.%m %H:%M')}")
    log.info("="*55)

    symbols = get_symbols()
    if not symbols:
        log.error("Нет символов"); return

    pf = get_prefilter()
    if pf:
        candidates = [s for s in symbols
                      if s in pf
                      and pf[s]['pct'] < PRICE_MAX * 2
                      and pf[s]['vol'] > 100000]
    else:
        candidates = symbols
    log.info(f"Кандидатов: {len(candidates)}")

    results = []
    done = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(scan_symbol, s): s for s in candidates}
        for f in as_completed(futures):
            done += 1
            try:
                sc, d = f.result()
                if sc >= MIN_SCORE and d:
                    results.append(d)
                    log.info(f"  ✓ {d['symbol']:15s} score={sc} база={d.get('base_days','?')}д OI+{d.get('oi12','?')}%")
            except Exception as e:
                log.debug(f"Err: {e}")

            if done % 50 == 0:
                log.info(f"Прогресс {done}/{len(candidates)} | сигналов: {len(results)}")

            # Промежуточная отправка
            if done == 300 and len(results) >= 3:
                results.sort(key=lambda x: x['score'], reverse=True)
                tg_send(fmt_alert(results[:TOP_RESULTS]))
                log.info("Промежуточный TG отправлен")

    results.sort(key=lambda x: x['score'], reverse=True)
    top = results[:TOP_RESULTS]

    log.info(f"РЕЗУЛЬТАТ: {len(results)} сигналов")

    if top:
        tg_send(fmt_alert(top))
        log.info("TG отправлен")
    else:
        tg_send(f"ℹ️ *ScreenerLabs*\n_{datetime.now().strftime('%d.%m %H:%M')} · нет сигналов_\n_Проверено: {len(candidates)} пар_")

    log.info("="*55)

# ── ЗАПУСК ────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    if len(sys.argv) == 3 and sys.argv[1] == "DEBUG":
        sym = sys.argv[2].upper()
        log.info(f"DEBUG: {sym}")
        sc, d = scan_symbol(sym)
        log.info(f"Score: {sc}")
        for k, v in d.items():
            log.info(f"  {k}: {v}")
    else:
        log.info("SCREENER LABS v2 запущен")
        run_scan()
        scheduler = BlockingScheduler()
        scheduler.add_job(run_scan, 'interval', hours=SCAN_HOURS)
        try:
            scheduler.start()
        except KeyboardInterrupt:
            log.info("Стоп")
