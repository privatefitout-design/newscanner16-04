"""
╔══════════════════════════════════════════════════════════════╗
║           CRYPTO SCANNER v5 — PRE-BREAKOUT ONLY             ║
║                  ⚡ ПАТТЕРН D                                ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║  ТРЁХФАЗНОЕ НАКОПЛЕНИЕ:                                      ║
║                                                              ║
║  Фаза 1  СТОИТ ДОЛГО    цена в узком диапазоне 20-180д       ║
║                         OI тихий, объём мёртвый              ║
║                         Нет даунтренда перед базой           ║
║                                                              ║
║  Фаза 2  OI РАСТЁТ      цена почти не двигается  ← МЫ ЗДЕСЬ ║
║                         OI плавно растёт +20%+ за 24-48ч    ║
║                         Умные деньги тихо набирают позицию   ║
║                                                              ║
║  Фаза 3  БУМ            пробой — сканер уже оповестил        ║
║                                                              ║
╠══════════════════════════════════════════════════════════════╣
║  Данные:   Binance Futures API (бесплатно, без ключа)        ║
║  Алерты:   Telegram                                          ║
║  Интервал: каждый час                                        ║
╚══════════════════════════════════════════════════════════════╝
"""

import time
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from apscheduler.schedulers.blocking import BlockingScheduler

# ══════════════════════════════════════════════════════════════
#  НАСТРОЙКИ
# ══════════════════════════════════════════════════════════════

TELEGRAM_TOKEN  = "8731868942:AAEKTM-hbrskq52V3wFtoKfUEr2Hn5-mrHQ"
CHAT_ID         = "181943757"

MIN_SCORE       = 25    # порог срабатывания
TOP_RESULTS     = 7     # топ сигналов в одном сообщении
SCAN_HOURS      = 0.5   # каждые 30 минут
SLEEP_REQ       = 0.05  # пауза между запросами
WORKERS         = 8     # параллельных потоков

# Пороги фазы 2 — можно настраивать
OI_24H_MIN      = 3     # минимальный рост OI за 3-6ч (%)
PRICE_CHG_MAX   = 25    # максимальное изменение цены за 6ч (%)
BASE_RANGE_MAX  = 80    # максимальный диапазон базы (%)
DOWNTREND_MAX   = -35   # порог даунтренда перед базой (%)

# ══════════════════════════════════════════════════════════════
#  ЛОГИРОВАНИЕ
# ══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
#  BINANCE API
# ══════════════════════════════════════════════════════════════

BASE = "https://fapi.binance.com"

def api(url, params=None):
    try:
        r = requests.get(url, params=params, timeout=10)
        return r.json() if r.status_code == 200 else None
    except:
        return None

def get_symbols():
    data = api(f"{BASE}/fapi/v1/exchangeInfo")
    if not data:
        return []
    return [
        s['symbol'] for s in data['symbols']
        if s['quoteAsset'] == 'USDT'
        and s['status'] == 'TRADING'
        and s['contractType'] == 'PERPETUAL'
    ]

def klines(symbol, interval, limit):
    data = api(f"{BASE}/fapi/v1/klines",
               {"symbol": symbol, "interval": interval, "limit": limit})
    if not data or not isinstance(data, list) or len(data) < 5:
        return None
    df = pd.DataFrame(data, columns=[
        'open_time','open','high','low','close','volume',
        'close_time','quote_vol','trades',
        'taker_buy_base','taker_buy_quote','ignore'
    ])
    for col in ['open','high','low','close','volume']:
        df[col] = df[col].astype(float)
    return df

def oi_hist(symbol, period, limit):
    data = api(f"{BASE}/futures/data/openInterestHist",
               {"symbol": symbol, "period": period, "limit": limit})
    if not data or not isinstance(data, list):
        return None
    df = pd.DataFrame(data)
    df['oi'] = df['sumOpenInterest'].astype(float)
    return df

# ══════════════════════════════════════════════════════════════
#  УТИЛИТЫ
# ══════════════════════════════════════════════════════════════

def oi_growth(oi_df, days):
    """Рост OI: первая половина периода vs вторая."""
    if oi_df is None or len(oi_df) < max(days, 8):
        return 0, False
    sl    = oi_df.iloc[-days:]
    half  = len(sl) // 2
    early = sl['oi'].iloc[:half].mean()
    late  = sl['oi'].iloc[half:].mean()
    g     = round((late - early) / early * 100, 1) if early > 0 else 0
    return g, g >= 10

def calc_natr(df, n=14):
    h, l, c = df['high'], df['low'], df['close']
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    return (tr.rolling(n).mean() / c * 100).round(3)

def oi_slope_angle(oi_series):
    """
    Угол наклона роста OI через линейную регрессию.
    Нормализуем значения чтобы получить реальный угол.
    0-90°: 45-65° = идеальное планомерное накопление.
    """
    try:
        y = oi_series.values.astype(float)
        if len(y) < 3:
            return 0
        x = np.arange(len(y))
        mn = y.min(); mx = y.max()
        y_norm = (y - mn) / (mx - mn) if mx != mn else np.zeros_like(y)
        slope, _ = np.polyfit(x, y_norm, 1)
        angle = round(np.degrees(np.arctan(slope * len(y))), 1)
        return max(0, min(90, angle))
    except:
        return 0

def no_downtrend(k1d, pre_days=90, skip_last=20):
    """
    Защита от нисходящего накопления.
    True = нет сильного даунтренда перед текущей зоной.
    """
    if k1d is None or len(k1d) < pre_days:
        return True
    c     = k1d['close']
    start = c.iloc[-pre_days]
    end   = c.iloc[-skip_last]
    trend = (end - start) / start * 100
    return trend > DOWNTREND_MAX

# ══════════════════════════════════════════════════════════════
#  ПАТТЕРН D — PRE-BREAKOUT ⚡
# ══════════════════════════════════════════════════════════════

def pattern_d(symbol, k1d, k1h, oi_d, oi_1h):
    """
    ФАЗА 1 — БАЗА (дневной ТФ):
      Цена стояла в узком диапазоне — NATR низкий и стабильный.
      Перед базой был аптренд или боковик (не даунтренд).
      Объём тихий и равномерный.

    ФАЗА 2 — ПРОБУЖДЕНИЕ (1H ТФ):
      NATR начинает расти — рынок просыпается.
      OI растёт равномерно 3-6ч под углом 45-65°.
      Цена почти не двигается — накопление ещё не завершено.

    СИГНАЛ: тихий флет + OI набирается + NATR просыпается
            → пробой ожидается в ближайшие часы.
    """

    if k1d is None or k1h is None or oi_1h is None:
        return 0, {}
    if len(k1d) < 25 or len(k1h) < 12 or len(oi_1h) < 6:
        return 0, {}

    score = 0
    d     = {"pattern": "D", "symbol": symbol}

    price_now = k1d['close'].iloc[-1]
    d['price_now'] = round(price_now, 8)

    # ── ФАЗА 1: БАЗА — NATR COMPRESSION ───────────────────────

    # Перед базой должен быть аптренд или боковик
    # Смотрим 90 дней назад до последних 20 — цена не должна падать
    if len(k1d) >= 90:
        pre_start = k1d['close'].iloc[-90]
        pre_end   = k1d['close'].iloc[-20]
        pre_trend = round((pre_end - pre_start) / pre_start * 100, 1)
        d['pre_trend_pct'] = pre_trend
        if pre_trend < -20:
            return 0, {}  # сильный даунтренд — не наш паттерн
    else:
        d['pre_trend_pct'] = None

    # NATR на дневном — должен быть низким (флет)
    natr_1d = calc_natr(k1d, n=14).dropna()
    if len(natr_1d) < 20:
        return 0, {}

    natr_base_mean = round(natr_1d.iloc[-30:-3].mean(), 2)  # среднее за базу
    natr_now_1d    = round(natr_1d.iloc[-1], 2)
    d['natr_base'] = natr_base_mean
    d['natr_now_1d'] = natr_now_1d

    # База: NATR был низким — цена стояла тихо
    if natr_base_mean > 8:
        return 0, {}  # слишком волатильная база
    if natr_base_mean < 1:   score += 30
    elif natr_base_mean < 2: score += 24
    elif natr_base_mean < 4: score += 16
    elif natr_base_mean < 8: score += 8

    # NATR на 1H — смотрим пробуждение
    natr_1h = calc_natr(k1h, n=7).dropna()
    if len(natr_1h) >= 12:
        natr_1h_base = round(natr_1h.iloc[-12:-3].mean(), 2)  # база на 1H
        natr_1h_now  = round(natr_1h.iloc[-3:].mean(), 2)     # последние 3 часа
        natr_awaken  = round(natr_1h_now / natr_1h_base, 2) if natr_1h_base > 0 else 1
        d['natr_1h_base']   = natr_1h_base
        d['natr_1h_now']    = natr_1h_now
        d['natr_awakening'] = natr_awaken

        # Пробуждение: NATR начал расти — рынок просыпается
        if natr_awaken > 3:
            return 0, {}  # слишком резко — уже памп
        if natr_awaken > 1.8:   score += 25  # активное пробуждение
        elif natr_awaken > 1.3: score += 16  # начинает просыпаться
        elif natr_awaken > 1.0: score += 8   # чуть шевелится
    else:
        d['natr_awakening'] = None

    # Диапазон базы на дневном
    base_window = min(60, len(k1d) - 3)
    base        = k1d.iloc[-base_window:-3]
    base_high   = base['high'].max()
    base_low    = base['low'].min()
    base_range  = round((base_high - base_low) / base_low * 100, 1)
    d['base_days']      = base_window
    d['base_range_pct'] = base_range

    if base_range > BASE_RANGE_MAX:
        return 0, {}
    if price_now < base_low * 0.95:
        return 0, {}

    # Тихий объём в базе
    vol_mean = base['volume'].mean()
    vol_cv   = base['volume'].std() / vol_mean if vol_mean > 0 else 99
    d['vol_cv'] = round(vol_cv, 2)
    if vol_cv < 0.5:   score += 10
    elif vol_cv < 0.8: score += 5

    # ── ФАЗА 2: OI НАКОПЛЕНИЕ + УГОЛ ──────────────────────────
    # Главное окно: 6-12 часов (именно там виден плавный рост)
    # На дневке это выглядит как spike, но внутри — дуга 45-55°

    oi_now_val = oi_1h['oi'].iloc[-1]

    # БАЗА OI — была ли тихой последние 30 дней
    if oi_d is not None and len(oi_d) >= 20:
        oi_30d      = oi_d['oi'].iloc[-30:]
        oi_30d_mean = oi_30d.mean()
        oi_30d_std  = oi_30d.std()
        oi_30d_cv   = round(oi_30d_std / oi_30d_mean, 3) if oi_30d_mean > 0 else 99
        d['oi_base_cv'] = oi_30d_cv
        # Тихая база: CV < 0.2 = OI был стабильным
        if oi_30d_cv < 0.15:  score += 15
        elif oi_30d_cv < 0.25: score += 8

    # OI рост за разные окна
    def oi_chg(n):
        if len(oi_1h) < n: return 0
        v = oi_1h['oi'].iloc[-n]
        return round((oi_now_val - v) / v * 100, 1) if v > 0 else 0

    oi_3h  = oi_chg(3)
    oi_6h  = oi_chg(6)
    oi_12h = oi_chg(12)  # главное окно
    oi_best = max(oi_3h, oi_6h, oi_12h)

    d['oi_3h_growth_pct']  = oi_3h
    d['oi_6h_growth_pct']  = oi_6h
    d['oi_12h_growth_pct'] = oi_12h
    d['oi_24h_growth_pct'] = oi_best  # для алерта

    if oi_best < OI_24H_MIN:
        return 0, {}

    # Скоринг — 12-часовой рост важнее краткосрочного
    if oi_12h > 50:    score += 40
    elif oi_12h > 30:  score += 32
    elif oi_12h > 15:  score += 22
    elif oi_6h > 20:   score += 18
    elif oi_6h > 10:   score += 12
    elif oi_3h > 8:    score += 8

    # OI должен активно расти прямо сейчас — не просто не падать
    # OI сейчас должен быть минимум на 2% выше чем 3 часа назад
    if len(oi_1h) >= 3:
        oi_now_check = oi_1h['oi'].iloc[-1]
        oi_3h_check  = oi_1h['oi'].iloc[-3]
        if oi_now_check < oi_3h_check * 1.005:
            return 0, {}  # OI не растёт активно прямо сейчас
        oi_last = oi_1h['oi'].iloc[-1]
        oi_prev = oi_1h['oi'].iloc[-2]
        oi_prev2 = oi_1h['oi'].iloc[-3]
        # Две последние свечи OI падают = поздно
        if oi_last < oi_prev and oi_prev < oi_prev2:
            return 0, {}  # OI снижается — памп уже закончился
        d['oi_falling'] = oi_last < oi_prev
    else:
        d['oi_falling'] = False

    # EMA20 фильтр — не должна падать (даунтренд)
    ema20 = k1d['close'].ewm(span=20, adjust=False).mean()
    if len(ema20) >= 5 and ema20.iloc[-1] < ema20.iloc[-4]:
        return 0, {}  # EMA20 падает — пропуск

    # OI/цена ratio — мощный рост OI при тихой цене = бонус
    if price_chg_6h > 0.1:
        oi_price_ratio = round(oi_best / price_chg_6h, 1)
    else:
        oi_price_ratio = oi_best * 10  # цена стоит — ratio очень высокий
    d['oi_price_ratio'] = oi_price_ratio
    if oi_price_ratio > 50:   score += 15  # OI растёт сильно, цена стоит
    elif oi_price_ratio > 20: score += 10
    elif oi_price_ratio > 10: score += 5

    # Угол наклона OI на 6h и 12h окнах
    angle_6h  = oi_slope_angle(oi_1h['oi'].iloc[-6:])
    angle_12h = oi_slope_angle(oi_1h['oi'].iloc[-12:]) if len(oi_1h) >= 12 else 0
    d['oi_angle_6h']  = angle_6h
    d['oi_angle_12h'] = angle_12h

    # Угол как фильтр — слишком вертикально = spike/памп
    best_angle = max(angle_6h, angle_12h)
    if best_angle > 78:
        return 0, {}  # вертикальный spike

    # Равномерность OI за 12 часов — не spike
    oi_window = min(12, len(oi_1h))
    oi_changes = oi_1h['oi'].iloc[-oi_window:].pct_change().dropna()
    oi_smooth  = round(oi_changes.std() * 100, 2)
    d['oi_smooth'] = oi_smooth
    if oi_smooth < 3:    score += 15
    elif oi_smooth < 6:  score += 9
    elif oi_smooth < 10: score += 4

    # Цена почти не двигается за 6 часов — главный критерий
    p6 = k1h['close'].iloc[-6] if len(k1h) >= 6 else k1h['close'].iloc[0]
    price_chg_6h = abs(round((price_now - p6) / p6 * 100, 2)) if p6 > 0 else 99
    d['price_chg_24h_pct'] = price_chg_6h

    if price_chg_6h > PRICE_CHG_MAX:
        return 0, {}
    if price_chg_6h < 1.5:  score += 25
    elif price_chg_6h < 3:  score += 18
    elif price_chg_6h < 5:  score += 10
    elif price_chg_6h < 10: score += 4

    # OI на дневном тоже растёт — подтверждение
    oi_daily_g, oi_daily_ok = oi_growth(oi_d, days=7)
    d['oi_daily_growth_pct'] = oi_daily_g
    if oi_daily_ok:
        if oi_daily_g > 40:   score += 12
        elif oi_daily_g > 20: score += 8
        else:                  score += 4

    d['score'] = score
    return score, d

# ══════════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════════

def tg_send(text):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10
        )
    except Exception as e:
        log.error(f"TG error: {e}")

def tg_send_chart(sym, caption):
    """Отправляет скриншот чарта в Telegram через TradingView image API."""
    try:
        # Пробуем TradingView
        chart_url = (
            f"https://charts.tradingview.com/chart-image/"
            f"?symbol=BINANCE:{sym}.P"
            f"&interval=60&width=800&height=400&theme=dark"
        )
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
            json={
                "chat_id": CHAT_ID,
                "photo": chart_url,
                "caption": caption,
                "parse_mode": "Markdown"
            },
            timeout=15
        )
        if r.status_code == 200 and r.json().get('ok'):
            return True

        # Запасной вариант — Binance chart
        chart_url2 = f"https://bin.bnbstatic.com/image/admin_mgs_image_upload/20240101/{sym}.png"
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
            json={
                "chat_id": CHAT_ID,
                "photo": chart_url2,
                "caption": caption,
                "parse_mode": "Markdown"
            },
            timeout=15
        )
        return True
    except Exception as e:
        log.error(f"Chart send error {sym}: {e}")
        return False

def fmt_signal(r, rank):
    sym   = r['symbol']
    score = r.get('score', 0)
    pchg  = r.get('price_chg_24h_pct', '?')
    brange= r.get('base_range_pct', '?')
    bdays = r.get('base_days', '?')
    oi12  = r.get('oi_12h_growth_pct', '?')
    angle = r.get('oi_angle_12h', r.get('oi_angle_6h', '?'))

    if score >= 80:   conf = "🔥"
    elif score >= 65: conf = "✅"
    else:             conf = "⚡"

    lines = [
        f"🟢 *LONG · Score: {score}* {conf}",
        f"{'─' * 24}",
        f"*{sym}*",
        f"📐 OI угол: `{angle}°` ↗️",
        f"📦 База: `{bdays}д` · сжатие `{brange}%`",
        f"OI `+{oi12}%` (12ч) · цена спит `±{pchg}%`",
        f"",
    ]
    return "\n".join(lines)

def fmt_alert(results, total_scanned):
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    msg = f"_{now}_\n\n"
    for i, r in enumerate(results, 1):
        msg += fmt_signal(r, i)
    return msg

# ══════════════════════════════════════════════════════════════
#  ОСНОВНОЙ СКАНЕР
# ══════════════════════════════════════════════════════════════

def get_prefilter():
    """
    Быстрый предфильтр — один запрос на все 538 пар.
    Оставляем только пары где цена почти не двигалась за 24ч.
    Отсекает ~80% символов до детального скана.
    """
    try:
        data = api(f"{BASE}/fapi/v1/ticker/24hr")
        if not data or not isinstance(data, list):
            return None
        result = {}
        for t in data:
            if not t['symbol'].endswith('USDT'):
                continue
            pct = abs(float(t.get('priceChangePercent', 99)))
            vol = float(t.get('quoteVolume', 0))
            result[t['symbol']] = {
                'price_chg_24h': pct,
                'volume_24h': vol
            }
        return result
    except:
        return None

def scan_symbol(sym):
    """Полная проверка одного символа — запускается параллельно."""
    try:
        k1d  = klines(sym, "1d",  200)
        k1h  = klines(sym, "1h",   50)
        oi_d = oi_hist(sym, "1d",  60)
        oi_1h= oi_hist(sym, "1h",  50)

        score, d = pattern_d(sym, k1d, k1h, oi_d, oi_1h)
        return score, d
    except Exception as e:
        log.debug(f"Ошибка {sym}: {e}")
        return 0, {}

def run_scan():
    log.info("=" * 55)
    log.info(f"PRE-BREAKOUT SCANNER  {datetime.now().strftime('%d.%m %H:%M')}")
    log.info("=" * 55)

    symbols = get_symbols()
    if not symbols:
        log.error("Нет символов")
        return
    log.info(f"Всего символов: {len(symbols)}")

    # ── ШАГ 1: БЫСТРЫЙ ПРЕДФИЛЬТР (1 запрос) ──────────────────
    log.info("Предфильтр — загружаем все тикеры...")
    prefilter = get_prefilter()

    if prefilter:
        # Оставляем только пары где:
        # - цена изменилась менее чем на PRICE_CHG_MAX% за 24ч
        # - есть хоть какой-то объём
        candidates = [
            s for s in symbols
            if s in prefilter
            and prefilter[s]['price_chg_24h'] < PRICE_CHG_MAX
            and prefilter[s]['volume_24h'] > 4000000
        ]
        log.info(f"После предфильтра: {len(candidates)} кандидатов (было {len(symbols)})")
    else:
        candidates = symbols
        log.info("Предфильтр недоступен — сканируем все")

    # ── ШАГ 2: ПАРАЛЛЕЛЬНЫЙ ДЕТАЛЬНЫЙ СКАН ────────────────────
    results = []
    errors  = 0
    done    = 0

    log.info(f"Запускаем {WORKERS} параллельных потоков...")

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(scan_symbol, sym): sym for sym in candidates}

        for future in as_completed(futures):
            sym = futures[future]
            done += 1
            try:
                score, d = future.result()
                if score >= MIN_SCORE and d:
                    results.append(d)
                    log.info(
                        f"  ⚡ {sym:15s}  score={score:3d}  "
                        f"NATR x{d.get('natr_awakening','?')}  "
                        f"OI +{d.get('oi_24h_growth_pct','?')}%  "
                        f"угол {d.get('oi_angle_4h','?')}°"
                    )
            except Exception as e:
                errors += 1
                log.debug(f"Ошибка {sym}: {e}")

            if done % 50 == 0:
                log.info(f"Прогресс {done}/{len(candidates)} | сигналов: {len(results)}")

    results.sort(key=lambda x: x['score'], reverse=True)
    top = results[:TOP_RESULTS]

    log.info(f"\nРЕЗУЛЬТАТ: {len(results)} сигналов | ошибок: {errors}")

    if top:
        for r in top:
            log.info(
                f"  → {r['symbol']:15s} score={r['score']}  "
                f"NATR x{r.get('natr_awakening','?')}  "
                f"OI +{r.get('oi_24h_growth_pct','?')}%  "
                f"угол {r.get('oi_angle_6h','?')}°"
            )
        # Отправляем общий текстовый алерт
        tg_send(fmt_alert(top, len(candidates)))

        # Отправляем чарт для каждого топ сигнала
        for r in top[:3]:  # максимум 3 чарта
            sym     = r['symbol']
            score   = r['score']
            oi_g    = r.get('oi_24h_growth_pct', '?')
            angle   = r.get('oi_angle_6h', '?')
            caption = (
                f"⚡ *{sym}*  Score: {score}\n"
                f"OI +{oi_g}%  Угол {angle}°"
            )
            sent = tg_send_chart(sym, caption)
            if sent:
                log.info(f"  📊 Chart sent: {sym}")
            time.sleep(1)  # пауза между фото

        log.info("Telegram отправлен")
    else:
        log.info("Сигналов нет")
        tg_send(
            f"ℹ️ *Pre-Breakout Scanner*\n"
            f"_{datetime.now().strftime('%d.%m %H:%M')} · нет сигналов_\n"
            f"_Проверено: {len(candidates)} пар_"
        )

    log.info("=" * 55)

# ══════════════════════════════════════════════════════════════
#  ЗАПУСК
# ══════════════════════════════════════════════════════════════

def debug_symbol(sym):
    """
    Режим отладки — показывает все значения для одного символа.
    Помогает понять почему символ не проходит фильтры.
    """
    log.info(f"\n{'='*55}")
    log.info(f"DEBUG: {sym}")
    log.info(f"{'='*55}")

    k1d  = klines(sym, "1d",  200)
    k1h  = klines(sym, "1h",   50)
    oi_d = oi_hist(sym, "1d",  60)
    oi_1h= oi_hist(sym, "1h",  50)

    # Предфильтр цены
    if k1h is not None and len(k1h) >= 6:
        p_now = k1h['close'].iloc[-1]
        p_6h  = k1h['close'].iloc[-6]
        chg   = abs(round((p_now - p_6h) / p_6h * 100, 2))
        log.info(f"Цена 6ч изменение: ±{chg}% (макс {PRICE_CHG_MAX}%) → {'OK' if chg < PRICE_CHG_MAX else 'ОТСЕВ'}")

    # Pre-trend
    if k1d is not None and len(k1d) >= 90:
        pre_s = k1d['close'].iloc[-90]
        pre_e = k1d['close'].iloc[-20]
        pt    = round((pre_e - pre_s) / pre_s * 100, 1)
        log.info(f"Тренд перед базой: {pt}% → {'OK' if pt >= -20 else 'ОТСЕВ (даунтренд)'}")

    # NATR
    if k1d is not None and len(k1d) >= 30:
        natr_d = calc_natr(k1d, 14).dropna()
        if len(natr_d) >= 20:
            nb = round(natr_d.iloc[-30:-3].mean(), 2)
            nn = round(natr_d.iloc[-1], 2)
            log.info(f"NATR дневной база: {nb} (макс 8) → {'OK' if nb < 8 else 'ОТСЕВ'}")
            log.info(f"NATR дневной сейчас: {nn}")

    if k1h is not None and len(k1h) >= 12:
        natr_h = calc_natr(k1h, 7).dropna()
        if len(natr_h) >= 12:
            nb = round(natr_h.iloc[-12:-3].mean(), 2)
            nn = round(natr_h.iloc[-3:].mean(), 2)
            aw = round(nn / nb, 2) if nb > 0 else 0
            log.info(f"NATR 1H база: {nb} → сейчас: {nn} → пробуждение: x{aw} (макс x3)")

    # OI
    if oi_1h is not None and len(oi_1h) >= 6:
        oi_now = oi_1h['oi'].iloc[-1]
        for h in [3, 6, 12]:
            if len(oi_1h) >= h:
                oi_ago = oi_1h['oi'].iloc[-h]
                g = round((oi_now - oi_ago) / oi_ago * 100, 1) if oi_ago > 0 else 0
                log.info(f"OI рост {h}ч: +{g}% (мин {OI_24H_MIN}%) → {'OK' if g >= OI_24H_MIN else 'МАЛО'}")

    if oi_d is not None and len(oi_d) >= 7:
        oi_g, oi_ok = oi_growth(oi_d, days=7)
        log.info(f"OI дневной рост 7д: +{oi_g}% → {'OK' if oi_ok else 'МАЛО'}")

    # Итоговый score
    score, d = pattern_d(sym, k1d, k1h, oi_d, oi_1h)
    log.info(f"\nИТОГ: score={score} (порог {MIN_SCORE}) → {'СИГНАЛ ✅' if score >= MIN_SCORE else 'НЕ ПРОШЁЛ ❌'}")
    if d:
        for k, v in d.items():
            if k not in ('pattern', 'symbol'):
                log.info(f"  {k}: {v}")
    log.info("="*55)

if __name__ == "__main__":
    import sys

    # Режим отладки: python scanner1_accumulation.py DEBUG INUSDT
    if len(sys.argv) == 3 and sys.argv[1] == "DEBUG":
        debug_symbol(sys.argv[2].upper())
    else:
        log.info("⚡ PRE-BREAKOUT SCANNER v5 запущен")
        log.info(f"Порог: score>={MIN_SCORE} | OI рост>={OI_24H_MIN}% | Цена<={PRICE_CHG_MAX}%")
        log.info(f"Интервал: каждые 30 минут")

        run_scan()

        scheduler = BlockingScheduler()
        scheduler.add_job(run_scan, 'interval', hours=SCAN_HOURS)
        log.info(f"Следующий скан через {SCAN_HOURS}ч")

        try:
            scheduler.start()
        except KeyboardInterrupt:
            log.info("Остановлено (Ctrl+C)")
