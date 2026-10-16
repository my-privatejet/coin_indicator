from __future__ import annotations

import copy
import json
import pickle
import random
import sqlite3
import threading
import time
import zlib
from abc import ABC
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Optional

import pandas as pd
import requests
import websocket


# ============================================================
# COMMON RUNTIME
# ============================================================

KST = timezone(timedelta(hours=9))

TELEGRAM_BOT_TOKEN = "8948914314:AAFFmncI-LSF6C_4YSeq1Nhn1Np9p3WulKc"
SYMBOL_GROUPS: list[dict] = []
ALL_SYMBOLS: list[str] = []
RUNTIME_CONFIGURED = False

_AUTO_DISCOVER_SYMBOLS = False
_USDT_ONLY = True
_EXCLUDE_SYMBOLS: set[str] = set()

# ============================================================
# COMMON SYMBOL CATEGORIES
# ============================================================
# 모든 지표가 공통으로 사용하는 종목 분류.
# MAJOR/MEME는 기준 목록만 관리하고, ALT는 OKX live USDT-SWAP 전체에서
# MAJOR/MEME를 제외해 resolve_runtime_symbols()에서 자동 생성한다.

MAJOR_SYMBOLS: set[str] = {
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "XRP-USDT-SWAP",
}

MEME_SYMBOLS: set[str] = {
    "BOME-USDT-SWAP",
    "BONK-USDT-SWAP",
    "BRETT-USDT-SWAP",
    "CASHCAT-USDT-SWAP",
    "DOGE-USDT-SWAP",
    "FARTCOIN-USDT-SWAP",
    "FLOKI-USDT-SWAP",
    "GIGGLE-USDT-SWAP",
    "JELLYJELLY-USDT-SWAP",
    "MEW-USDT-SWAP",
    "MOODENG-USDT-SWAP",
    "MUBARAK-USDT-SWAP",
    "NEIRO-USDT-SWAP",
    "PENGU-USDT-SWAP",
    "PEPE-USDT-SWAP",
    "PIPPIN-USDT-SWAP",
    "PNUT-USDT-SWAP",
    "POPCAT-USDT-SWAP",
    "PURR-USDT-SWAP",
    "SHIB-USDT-SWAP",
    "SPX-USDT-SWAP",
    "TRUMP-USDT-SWAP",
    "TURBO-USDT-SWAP",
    "USELESS-USDT-SWAP",
    "WEN-USDT-SWAP",
    "WIF-USDT-SWAP",
}

ALT_SYMBOLS: set[str] = set()


def make_common_symbol_groups(
    *,
    major_chat_id: str,
    meme_chat_id: str,
    alt_chat_id: str,
) -> list[dict]:
    """
    지표 파일에서 공통 MAJOR/MEME/ALT 분류를 사용할 때 호출한다.
    실제 symbols는 OKX 조회 후 resolve_runtime_symbols()에서 채운다.
    """
    return [
        {
            "name": "MAJOR",
            "symbols": set(),
            "chat_id": major_chat_id,
            "catch_all": False,
            "priority": 0,
            "common_category": "MAJOR",
        },
        {
            "name": "MEME",
            "symbols": set(),
            "chat_id": meme_chat_id,
            "catch_all": False,
            "priority": 1,
            "common_category": "MEME",
        },
        {
            "name": "ALT",
            "symbols": set(),
            "chat_id": alt_chat_id,
            "catch_all": False,
            "priority": 2,
            "common_category": "ALT",
        },
    ]


# ============================================================
# RUNTIME DEFAULTS
# ============================================================

TIMEFRAMES = ["5m", "15m", "1h", "4h", "1d"]

REQUEST_TIMEOUT = 10

# REST startup / recovery
MAX_WORKERS = 8
INIT_MAX_WORKERS = 4
RECOVERY_MAX_WORKERS = 4
RECENT_LIMIT = 20
HISTORY_REQUESTS_PER_2S = 16
RECENT_REQUESTS_PER_2S = 30
HTTP_MAX_RETRIES = 6

STATE_FILE = Path("common_state_okx.json")
CANDLE_CACHE_FILE = Path("common_candle_cache.pkl")
DB_FILE = Path("common_state_okx.sqlite3")

OKX_PUBLIC_WS = "wss://ws.okx.com/ws/v5/public"
OKX_BUSINESS_WS = "wss://ws.okx.com/ws/v5/business"

OKX_INSTRUMENTS_URL = "https://www.okx.com/api/v5/public/instruments"
OKX_HISTORY_URL = "https://www.okx.com/api/v5/market/history-candles"
OKX_CANDLES_URL = "https://www.okx.com/api/v5/market/candles"

WS_RECONNECT_SECONDS = 2.0
WS_SUBSCRIBE_CHUNK = 50
TRADE_WS_CONNECTIONS = 4
CANDLE_WS_CONNECTIONS = 4
WS_CONNECTION_START_GAP = 0.40

# Same symbol -> same worker index.
# Candle and trade queues are physically separated.
REALTIME_WORKERS = 16
REALTIME_QUEUE_WARN_SIZE = 5_000
REALTIME_LAG_WARN_MS = 2_000

# Candle processing retry. Checkpoint never advances on failure.
CANDLE_PROCESS_MAX_IMMEDIATE_RETRIES = 5
CANDLE_PROCESS_RETRY_SECONDS = 1.0

# GAP recovery
GAP_RECOVERY_ATTEMPTS = 6
GAP_RECOVERY_MAX_BARS = 5000

# Telegram durable outbox
TELEGRAM_WORKERS = 8
TELEGRAM_RETRY_MAX_SECONDS = 60.0
TELEGRAM_POLL_SECONDS = 0.20

CACHE_FLUSH_SECONDS = 60.0
STATE_FLUSH_SECONDS = 5.0
SHUTDOWN_DRAIN_SECONDS = 10.0

# For production: every indicator must support pair-state snapshot/rollback.
REQUIRE_TRANSACTIONAL_INDICATORS = True


# ============================================================
# TIMEFRAME SETTINGS
# ============================================================

OKX_INTERVAL = {
    "5m": "5m",
    "15m": "15m",
    "1h": "1H",
    "4h": "4H",
    "1d": "1D",
}

OKX_CANDLE_CHANNEL = {
    "5m": "candle5m",
    "15m": "candle15m",
    "1h": "candle1H",
    "4h": "candle4H",
    "1d": "candle1D",
}

TIMEFRAME_SECONDS = {
    "5m": 5 * 60,
    "15m": 15 * 60,
    "1h": 60 * 60,
    "4h": 4 * 60 * 60,
    "1d": 24 * 60 * 60,
}


# ============================================================
# DATA MODELS
# ============================================================

@dataclass
class Signal:
    indicator: str
    signal_id: str
    symbol: str
    timeframe: str
    side: str
    price: Optional[float] = None
    title: Optional[str] = None
    message: Optional[str] = None
    extra: dict[str, Any] = field(default_factory=dict)
    ts_ms: Optional[int] = None


@dataclass
class LiveBar:
    bucket: int
    open: float
    high: float
    low: float
    close: float
    first_ts_ms: int
    last_ts_ms: int


@dataclass
class RealtimeEvent:
    kind: str
    symbol: str
    enqueued_mono: float
    price: Optional[float] = None
    ts_ms: Optional[int] = None
    timeframe: Optional[str] = None
    candle: Optional[dict] = None
    attempt: int = 0


class BaseIndicator(ABC):
    """
    Indicator contract.

    Production rule:
    transactional_pair_state=True indicators must implement
    snapshot_pair_state() and restore_pair_state().
    The engine snapshots pair state before a live/candle operation and
    restores it if the durable DB transaction fails.
    """

    name = "base"
    display_name = "BASE"
    use_live_trade = False
    history_limit = 300
    transactional_pair_state = False

    def startup(self) -> None:
        pass

    def shutdown(self) -> None:
        pass

    def on_candle(
        self,
        symbol: str,
        timeframe: str,
        df: pd.DataFrame,
        current_candle: Optional[dict],
        startup: bool,
    ) -> list[Signal]:
        return []

    def on_trade(
        self,
        symbol: str,
        timeframe: str,
        price: float,
        ts_ms: int,
        live_bar: LiveBar,
    ) -> list[Signal]:
        return []

    def snapshot_pair_state(self, symbol: str, timeframe: str) -> Any:
        return None

    def restore_pair_state(self, symbol: str, timeframe: str, snapshot: Any) -> None:
        pass

    def get_state(self) -> dict:
        return {}

    def load_state(self, state: dict) -> None:
        pass


# ============================================================
# GLOBAL STATE
# ============================================================

CACHE_LOCK = threading.RLock()
LIVE_BAR_LOCK = threading.RLock()
READY_LOCK = threading.RLock()
EVENT_GATE_LOCK = threading.RLock()
INDICATOR_STATE_LOCK = threading.RLock()

ENGINE_STOP = threading.Event()
CACHE_DIRTY = threading.Event()
STATE_DIRTY = threading.Event()
TELEGRAM_WAKE = threading.Event()

CANDLE_CACHE: dict[str, pd.DataFrame] = {}
LIVE_BARS: dict[str, LiveBar] = {}

# Durable checkpoint: only after indicator mutation + signal outbox + DB commit.
PROCESSED_OPEN_TIME: dict[str, int] = {}
LAST_CLOSED_OPEN_TIME = PROCESSED_OPEN_TIME

READY_PAIRS: set[str] = set()
RECOVERING_PAIRS: set[str] = set()

# pair_key -> {open_time: row}
GATED_CANDLE_BUFFER: dict[str, dict[int, dict]] = {}

INDICATORS: list[BaseIndicator] = []
LIVE_INDICATORS: list[BaseIndicator] = []
HISTORY_LIMIT = 300

# Separate queues: closed candles always have priority over trades.
CANDLE_QUEUES: list[Queue] = [Queue() for _ in range(REALTIME_WORKERS)]
TRADE_QUEUES: list[Queue] = [Queue() for _ in range(REALTIME_WORKERS)]
REALTIME_THREADS: list[threading.Thread] = []

TELEGRAM_THREADS: list[threading.Thread] = []

STORE: Optional["DurableStore"] = None


# ============================================================
# HELPERS
# ============================================================

def pair_key(symbol: str, timeframe: str) -> str:
    return f"okx_swap:{symbol}:{timeframe}"


def timeframe_bucket(timeframe: str, ts_ms: int) -> int:
    sec = TIMEFRAME_SECONDS[timeframe]
    shift = 8 * 3600 if timeframe == "1d" else 0
    return (ts_ms // 1000 + shift) // sec


def bucket_open_ms(timeframe: str, ts_ms: int) -> int:
    sec = TIMEFRAME_SECONDS[timeframe]
    shift = 8 * 3600 if timeframe == "1d" else 0
    bucket = (ts_ms // 1000 + shift) // sec
    return int((bucket * sec - shift) * 1000)


def expected_last_closed_open_ms(timeframe: str, now_ms: Optional[int] = None) -> int:
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    sec = TIMEFRAME_SECONDS[timeframe]
    shift = 8 * 3600 if timeframe == "1d" else 0
    current_bucket = (now_ms // 1000 + shift) // sec
    return int(((current_bucket - 1) * sec - shift) * 1000)


def normalize_symbol(symbol: str) -> str:
    s = str(symbol).strip().upper()
    if not s:
        return ""
    if s.endswith("-SWAP"):
        return s
    if s.endswith("USDT") and "-" not in s:
        return f"{s[:-4]}-USDT-SWAP"
    return s


def stable_worker_index(symbol: str) -> int:
    return zlib.crc32(symbol.encode("utf-8")) % REALTIME_WORKERS


def split_evenly(items: list[Any], parts: int) -> list[list[Any]]:
    if not items:
        return []
    parts = max(1, min(parts, len(items)))
    result = [[] for _ in range(parts)]
    for i, item in enumerate(items):
        result[i % parts].append(item)
    return result


def fmt_price(v: float) -> str:
    if v >= 1000:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:,.4f}".rstrip("0").rstrip(".")
    return f"{v:.10f}".rstrip("0").rstrip(".")


def is_pair_ready(symbol: str, timeframe: str) -> bool:
    with READY_LOCK:
        return pair_key(symbol, timeframe) in READY_PAIRS


def is_pair_recovering(symbol: str, timeframe: str) -> bool:
    with EVENT_GATE_LOCK:
        return pair_key(symbol, timeframe) in RECOVERING_PAIRS


def _mark_pair_ready(key: str) -> None:
    with READY_LOCK:
        READY_PAIRS.add(key)


def _mark_pair_not_ready(key: str) -> None:
    with READY_LOCK:
        READY_PAIRS.discard(key)


# ============================================================
# DURABLE SQLITE STORE
# ============================================================

class DurableStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(
            str(self.path),
            timeout=30.0,
            check_same_thread=False,
        )
        self.conn.row_factory = sqlite3.Row
        with self.lock:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=FULL")
            self.conn.execute("PRAGMA busy_timeout=30000")
            self._create_schema()
            self.conn.execute(
                """
                UPDATE telegram_outbox
                   SET status='PENDING', updated_at=?
                 WHERE status='SENDING'
                """,
                (int(time.time() * 1000),),
            )
            self.conn.commit()

    def _create_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS processed_candles (
                pair_key   TEXT PRIMARY KEY,
                open_time  INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS indicator_state (
                indicator  TEXT PRIMARY KEY,
                state_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS telegram_outbox (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_key    TEXT NOT NULL UNIQUE,
                group_name    TEXT NOT NULL,
                chat_id       TEXT NOT NULL,
                text          TEXT NOT NULL,
                status        TEXT NOT NULL DEFAULT 'PENDING',
                attempts      INTEGER NOT NULL DEFAULT 0,
                next_retry_at INTEGER NOT NULL DEFAULT 0,
                created_at    INTEGER NOT NULL,
                updated_at    INTEGER NOT NULL,
                last_error    TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_outbox_pending
            ON telegram_outbox(status, next_retry_at, id);
            """
        )
        self.conn.commit()

    def load_processed(self) -> dict[str, int]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT pair_key, open_time FROM processed_candles"
            ).fetchall()
        return {str(r["pair_key"]): int(r["open_time"]) for r in rows}

    def load_indicator_states(self) -> dict[str, dict]:
        result: dict[str, dict] = {}
        with self.lock:
            rows = self.conn.execute(
                "SELECT indicator, state_json FROM indicator_state"
            ).fetchall()
        for row in rows:
            try:
                value = json.loads(str(row["state_json"]))
                if isinstance(value, dict):
                    result[str(row["indicator"])] = value
            except Exception:
                pass
        return result

    @staticmethod
    def _insert_outbox_rows(cur: sqlite3.Cursor, rows: list[dict], now_ms: int) -> None:
        for row in rows:
            cur.execute(
                """
                INSERT OR IGNORE INTO telegram_outbox(
                    signal_key, group_name, chat_id, text,
                    status, attempts, next_retry_at, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, 'PENDING', 0, 0, ?, ?)
                """,
                (
                    row["signal_key"],
                    row["group_name"],
                    row["chat_id"],
                    row["text"],
                    now_ms,
                    now_ms,
                ),
            )

    @staticmethod
    def _upsert_indicator_states(
        cur: sqlite3.Cursor,
        states: dict[str, dict],
        now_ms: int,
    ) -> None:
        for name, state in states.items():
            cur.execute(
                """
                INSERT INTO indicator_state(indicator, state_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(indicator) DO UPDATE SET
                    state_json=excluded.state_json,
                    updated_at=excluded.updated_at
                """,
                (
                    name,
                    json.dumps(state, ensure_ascii=False, separators=(",", ":")),
                    now_ms,
                ),
            )

    def commit_candle_result(
        self,
        *,
        key: str,
        open_time: int,
        outbox_rows: list[dict],
        indicator_states: dict[str, dict],
    ) -> None:
        now_ms = int(time.time() * 1000)
        with self.lock:
            cur = self.conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")
                self._insert_outbox_rows(cur, outbox_rows, now_ms)
                self._upsert_indicator_states(cur, indicator_states, now_ms)
                cur.execute(
                    """
                    INSERT INTO processed_candles(pair_key, open_time, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(pair_key) DO UPDATE SET
                        open_time=CASE
                            WHEN excluded.open_time > processed_candles.open_time
                            THEN excluded.open_time
                            ELSE processed_candles.open_time
                        END,
                        updated_at=excluded.updated_at
                    """,
                    (key, int(open_time), now_ms),
                )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def commit_live_signals(
        self,
        *,
        outbox_rows: list[dict],
        indicator_states: dict[str, dict],
    ) -> None:
        if not outbox_rows and not indicator_states:
            return
        now_ms = int(time.time() * 1000)
        with self.lock:
            cur = self.conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")
                self._insert_outbox_rows(cur, outbox_rows, now_ms)
                self._upsert_indicator_states(cur, indicator_states, now_ms)
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def save_indicator_states(self, states: dict[str, dict]) -> None:
        now_ms = int(time.time() * 1000)
        with self.lock:
            cur = self.conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")
                self._upsert_indicator_states(cur, states, now_ms)
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def claim_outbox(self) -> Optional[dict]:
        now_ms = int(time.time() * 1000)
        with self.lock:
            cur = self.conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")
                row = cur.execute(
                    """
                    SELECT *
                      FROM telegram_outbox
                     WHERE status='PENDING'
                       AND next_retry_at <= ?
                     ORDER BY id
                     LIMIT 1
                    """,
                    (now_ms,),
                ).fetchone()
                if row is None:
                    self.conn.commit()
                    return None
                cur.execute(
                    """
                    UPDATE telegram_outbox
                       SET status='SENDING',
                           attempts=attempts+1,
                           updated_at=?
                     WHERE id=? AND status='PENDING'
                    """,
                    (now_ms, int(row["id"])),
                )
                if cur.rowcount != 1:
                    self.conn.rollback()
                    return None
                self.conn.commit()
                result = dict(row)
                result["attempts"] = int(row["attempts"]) + 1
                return result
            except Exception:
                self.conn.rollback()
                raise

    def mark_outbox_sent(self, row_id: int) -> None:
        with self.lock:
            self.conn.execute(
                """
                UPDATE telegram_outbox
                   SET status='SENT', updated_at=?, last_error=NULL
                 WHERE id=?
                """,
                (int(time.time() * 1000), int(row_id)),
            )
            self.conn.commit()

    def mark_outbox_retry(self, row_id: int, attempts: int, error: str) -> None:
        delay = min(
            TELEGRAM_RETRY_MAX_SECONDS,
            max(1.0, 2.0 ** min(max(attempts - 1, 0), 6)),
        )
        with self.lock:
            self.conn.execute(
                """
                UPDATE telegram_outbox
                   SET status='PENDING',
                       next_retry_at=?,
                       updated_at=?,
                       last_error=?
                 WHERE id=?
                """,
                (
                    int((time.time() + delay) * 1000),
                    int(time.time() * 1000),
                    str(error)[:2000],
                    int(row_id),
                ),
            )
            self.conn.commit()

    def pending_outbox_count(self) -> int:
        with self.lock:
            row = self.conn.execute(
                """
                SELECT COUNT(*) AS n
                  FROM telegram_outbox
                 WHERE status IN ('PENDING', 'SENDING')
                """
            ).fetchone()
        return int(row["n"]) if row else 0

    def prune_sent_outbox(self, older_than_days: int = 90) -> int:
        cutoff = int((time.time() - older_than_days * 86400) * 1000)
        with self.lock:
            cur = self.conn.execute(
                "DELETE FROM telegram_outbox WHERE status='SENT' AND updated_at < ?",
                (cutoff,),
            )
            self.conn.commit()
            return int(cur.rowcount)

    def close(self) -> None:
        with self.lock:
            self.conn.commit()
            self.conn.close()


# ============================================================
# SYMBOL DISCOVERY / GROUPING
# ============================================================

def fetch_all_okx_swap_symbols(usdt_only: bool = True) -> list[str]:
    r = requests.get(
        OKX_INSTRUMENTS_URL,
        params={"instType": "SWAP"},
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    payload = r.json()
    if str(payload.get("code")) != "0":
        raise RuntimeError(
            f"OKX instruments error: code={payload.get('code')} msg={payload.get('msg')}"
        )

    symbols: list[str] = []
    for item in payload.get("data") or []:
        if str(item.get("state")) != "live":
            continue
        inst_id = str(item.get("instId") or "").strip()
        if not inst_id:
            continue
        if usdt_only and not inst_id.endswith("-USDT-SWAP"):
            continue
        symbols.append(inst_id)
    return sorted(set(symbols))


def _normalize_symbol_groups(symbol_groups: list[dict]) -> list[dict]:
    normalized: list[dict] = []
    for index, group in enumerate(symbol_groups, start=1):
        symbols = {
            normalize_symbol(s)
            for s in (group.get("symbols") or [])
            if normalize_symbol(s)
        }
        normalized.append(
            {
                "name": str(group.get("name") or f"GROUP_{index}"),
                "symbols": symbols,
                "chat_id": str(group.get("chat_id") or "").strip(),
                "catch_all": bool(group.get("catch_all", False)),
                "priority": int(group.get("priority", index - 1)),
                "common_category": (
                    str(group.get("common_category") or "").strip().upper()
                    or None
                ),
            }
        )

    if sum(1 for g in normalized if g["catch_all"]) > 1:
        raise ValueError("catch_all=True 그룹은 하나만 지정할 수 있습니다.")

    # Explicit groups must not overlap.
    seen: set[str] = set()
    for group in normalized:
        if group["catch_all"]:
            continue
        overlap = seen & group["symbols"]
        if overlap:
            raise ValueError(f"그룹 중복 종목: {sorted(overlap)}")
        seen |= group["symbols"]

    return normalized


def configure_runtime(
    *,
    symbol_groups: list[dict],
    state_file: str | Path,
    candle_cache_file: str | Path,
    timeframes: Optional[list[str]] = None,
    auto_discover_symbols: bool = False,
    usdt_only: bool = True,
    exclude_symbols: Optional[set[str] | list[str] | tuple[str, ...]] = None,
) -> None:
    global SYMBOL_GROUPS, ALL_SYMBOLS
    global STATE_FILE, CANDLE_CACHE_FILE, DB_FILE, TIMEFRAMES
    global RUNTIME_CONFIGURED, _AUTO_DISCOVER_SYMBOLS, _USDT_ONLY, _EXCLUDE_SYMBOLS

    STATE_FILE = Path(state_file)
    CANDLE_CACHE_FILE = Path(candle_cache_file)
    DB_FILE = STATE_FILE.with_suffix(".sqlite3")

    if timeframes is not None:
        TIMEFRAMES = list(timeframes)

    if not symbol_groups:
        raise ValueError("symbol_groups가 비어 있습니다.")

    SYMBOL_GROUPS = _normalize_symbol_groups(symbol_groups)
    _AUTO_DISCOVER_SYMBOLS = bool(auto_discover_symbols)
    _USDT_ONLY = bool(usdt_only)
    _EXCLUDE_SYMBOLS = {
        normalize_symbol(s)
        for s in (exclude_symbols or [])
        if normalize_symbol(s)
    }

    if _AUTO_DISCOVER_SYMBOLS:
        ALL_SYMBOLS = []
    else:
        values: list[str] = []
        for group in SYMBOL_GROUPS:
            values.extend(group["symbols"])
        ALL_SYMBOLS = list(dict.fromkeys(values))

    RUNTIME_CONFIGURED = True


def resolve_runtime_symbols() -> None:
    global SYMBOL_GROUPS, ALL_SYMBOLS
    global MAJOR_SYMBOLS, MEME_SYMBOLS, ALT_SYMBOLS

    if not _AUTO_DISCOVER_SYMBOLS:
        return

    discovered = set(fetch_all_okx_swap_symbols(usdt_only=_USDT_ONLY))
    discovered -= _EXCLUDE_SYMBOLS

    # 공통 분류는 반드시 현재 OKX live 종목과 교집합만 사용한다.
    major_live = discovered & set(MAJOR_SYMBOLS)
    meme_live = discovered & set(MEME_SYMBOLS)

    overlap = major_live & meme_live
    if overlap:
        raise ValueError(f"COMMON MAJOR/MEME 중복 종목: {sorted(overlap)}")

    alt_live = discovered - major_live - meme_live

    # set 객체를 교체하지 않고 제자리 갱신한다.
    # common_engine.MAJOR_SYMBOLS 등을 import한 코드도 최신 내용을 볼 수 있다.
    MAJOR_SYMBOLS.clear()
    MAJOR_SYMBOLS.update(major_live)
    MEME_SYMBOLS.clear()
    MEME_SYMBOLS.update(meme_live)
    ALT_SYMBOLS.clear()
    ALT_SYMBOLS.update(alt_live)

    explicit_seen: set[str] = set()
    catch_all: Optional[dict] = None

    for group in SYMBOL_GROUPS:
        category = group.get("common_category")

        if category == "MAJOR":
            group["symbols"] = set(MAJOR_SYMBOLS)
            explicit_seen |= group["symbols"]
            continue
        if category == "MEME":
            group["symbols"] = set(MEME_SYMBOLS)
            explicit_seen |= group["symbols"]
            continue
        if category == "ALT":
            group["symbols"] = set(ALT_SYMBOLS)
            explicit_seen |= group["symbols"]
            continue

        if group["catch_all"]:
            catch_all = group
            continue

        requested = set(group["symbols"])
        overlap = explicit_seen & requested
        if overlap:
            raise ValueError(f"자동분류 그룹 중복 종목: {sorted(overlap)}")

        missing = requested - discovered
        if missing:
            print(f"[GROUP WARN] {group['name']} 현재 OKX live에 없음: {sorted(missing)}")

        group["symbols"] = discovered & requested
        explicit_seen |= group["symbols"]

    if catch_all is not None:
        catch_all["symbols"] = discovered - explicit_seen
        assigned = discovered
    else:
        assigned = set().union(*(set(g["symbols"]) for g in SYMBOL_GROUPS))

    ALL_SYMBOLS = sorted(assigned)

    print(
        f"[SYMBOLS] OKX live SWAP={len(discovered)}개 | "
        f"MAJOR={len(MAJOR_SYMBOLS)} | MEME={len(MEME_SYMBOLS)} | "
        f"ALT={len(ALT_SYMBOLS)} | 실제 감시={len(ALL_SYMBOLS)}"
    )


def get_telegram_targets_for_symbol(symbol: str) -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    seen_chat_ids: set[str] = set()
    for group in SYMBOL_GROUPS:
        if symbol not in group["symbols"]:
            continue
        chat_id = str(group["chat_id"]).strip()
        if not chat_id or chat_id in seen_chat_ids:
            continue
        seen_chat_ids.add(chat_id)
        result.append((str(group["name"]), chat_id))
    return result


# ============================================================
# HTTP RATE LIMIT
# ============================================================

class SlidingWindowRateLimiter:
    def __init__(self, max_calls: int, period: float = 2.0):
        self.max_calls = max_calls
        self.period = period
        self.calls = deque()
        self.lock = threading.Lock()

    def wait(self) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                while self.calls and now - self.calls[0] >= self.period:
                    self.calls.popleft()
                if len(self.calls) < self.max_calls:
                    self.calls.append(now)
                    return
                wait_for = self.period - (now - self.calls[0]) + 0.02
            time.sleep(max(wait_for, 0.02))


HISTORY_LIMITER = SlidingWindowRateLimiter(HISTORY_REQUESTS_PER_2S)
RECENT_LIMITER = SlidingWindowRateLimiter(RECENT_REQUESTS_PER_2S)
WS_CONNECT_LIMITER = SlidingWindowRateLimiter(3, 1.0)


def okx_get(url: str, params: dict, limiter: SlidingWindowRateLimiter) -> dict:
    last_error: Optional[Exception] = None

    for attempt in range(HTTP_MAX_RETRIES):
        limiter.wait()
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                try:
                    wait_seconds = float(retry_after) if retry_after else 0.0
                except Exception:
                    wait_seconds = 0.0
                wait_seconds = max(wait_seconds, min(2 ** attempt, 8))
                time.sleep(wait_seconds)
                continue

            r.raise_for_status()
            payload = r.json()

            if str(payload.get("code")) == "50011":
                time.sleep(min(2 ** attempt, 8))
                continue

            if str(payload.get("code")) != "0":
                raise RuntimeError(
                    f"OKX error {params.get('instId')} {params.get('bar')}: "
                    f"code={payload.get('code')} msg={payload.get('msg')}"
                )

            return payload

        except requests.RequestException as e:
            last_error = e
            if attempt >= HTTP_MAX_RETRIES - 1:
                raise
            time.sleep(min(2 ** attempt, 8))

    if last_error is not None:
        raise last_error
    raise RuntimeError("OKX request failed")


# ============================================================
# CANDLE / CACHE
# ============================================================

def candle_row(k: list, timeframe: str) -> Optional[dict]:
    if len(k) < 9:
        return None
    ts = int(k[0])
    duration_ms = TIMEFRAME_SECONDS[timeframe] * 1000
    return {
        "open_time": ts,
        "open": float(k[1]),
        "high": float(k[2]),
        "low": float(k[3]),
        "close": float(k[4]),
        "close_time": ts + duration_ms,
        "confirm": str(k[8]),
    }


def normalize_closed_rows(raw: list, timeframe: str) -> pd.DataFrame:
    rows_by_ts: dict[int, dict] = {}
    for k in raw:
        row = candle_row(k, timeframe)
        if row is None or row["confirm"] != "1":
            continue
        row.pop("confirm", None)
        rows_by_ts[int(row["open_time"])] = row

    rows = [rows_by_ts[ts] for ts in sorted(rows_by_ts)]
    if not rows:
        return pd.DataFrame(
            columns=["open_time", "open", "high", "low", "close", "close_time"]
        )
    return pd.DataFrame(rows).reset_index(drop=True)


def fetch_closed_klines(symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
    rows_by_ts: dict[int, dict] = {}
    after = None
    previous_oldest = None

    while len(rows_by_ts) < limit:
        params = {
            "instId": symbol,
            "bar": OKX_INTERVAL[timeframe],
            "limit": "100",
        }
        if after is not None:
            params["after"] = str(after)

        payload = okx_get(OKX_HISTORY_URL, params, HISTORY_LIMITER)
        raw = payload.get("data") or []
        if not raw:
            break

        batch_ts: list[int] = []
        for k in raw:
            row = candle_row(k, timeframe)
            if row is None:
                continue
            ts = int(row["open_time"])
            batch_ts.append(ts)
            if row["confirm"] != "1":
                continue
            row.pop("confirm", None)
            rows_by_ts[ts] = row

        if not batch_ts:
            break

        oldest = min(batch_ts)
        if previous_oldest is not None and oldest >= previous_oldest:
            break
        previous_oldest = oldest
        after = oldest

        if len(raw) < 100:
            break

    if not rows_by_ts:
        raise RuntimeError(f"No closed OKX klines: {symbol} {timeframe}")

    rows = [rows_by_ts[ts] for ts in sorted(rows_by_ts)]
    if len(rows) > limit:
        rows = rows[-limit:]
    return pd.DataFrame(rows).reset_index(drop=True)


def fetch_recent_bundle(
    symbol: str,
    timeframe: str,
    limit: int = RECENT_LIMIT,
) -> tuple[pd.DataFrame, Optional[dict]]:
    payload = okx_get(
        OKX_CANDLES_URL,
        {
            "instId": symbol,
            "bar": OKX_INTERVAL[timeframe],
            "limit": str(limit),
        },
        RECENT_LIMITER,
    )
    raw = payload.get("data") or []
    closed_df = normalize_closed_rows(raw, timeframe)

    current = None
    for k in raw:
        row = candle_row(k, timeframe)
        if row is not None and row["confirm"] == "0":
            current = row
            break

    return closed_df, current


def fetch_closed_since(
    symbol: str,
    timeframe: str,
    after_open_time: int,
    hard_limit: int = GAP_RECOVERY_MAX_BARS,
) -> list[dict]:
    rows_by_ts: dict[int, dict] = {}
    after_cursor = None
    previous_oldest = None

    while len(rows_by_ts) < hard_limit:
        params = {
            "instId": symbol,
            "bar": OKX_INTERVAL[timeframe],
            "limit": "100",
        }
        if after_cursor is not None:
            params["after"] = str(after_cursor)

        payload = okx_get(OKX_HISTORY_URL, params, HISTORY_LIMITER)
        raw = payload.get("data") or []
        if not raw:
            break

        batch_ts: list[int] = []
        for k in raw:
            row = candle_row(k, timeframe)
            if row is None:
                continue
            ts = int(row["open_time"])
            batch_ts.append(ts)
            if row["confirm"] != "1" or ts <= after_open_time:
                continue
            rows_by_ts[ts] = row

        if not batch_ts:
            break

        oldest = min(batch_ts)
        if oldest <= after_open_time:
            break
        if previous_oldest is not None and oldest >= previous_oldest:
            break
        previous_oldest = oldest
        after_cursor = oldest
        if len(raw) < 100:
            break

    return [rows_by_ts[ts] for ts in sorted(rows_by_ts)]


def merge_candles(
    old_df: Optional[pd.DataFrame],
    new_df: Optional[pd.DataFrame],
    history_limit: int,
) -> pd.DataFrame:
    if old_df is None or old_df.empty:
        merged = new_df.copy() if new_df is not None else pd.DataFrame()
    elif new_df is None or new_df.empty:
        merged = old_df.copy()
    else:
        merged = pd.concat([old_df, new_df], ignore_index=True)

    if merged.empty:
        return merged

    return (
        merged
        .drop_duplicates(subset=["open_time"], keep="last")
        .sort_values("open_time")
        .tail(history_limit)
        .reset_index(drop=True)
    )


def cache_is_contiguous(
    df: Optional[pd.DataFrame],
    timeframe: str,
    required_length: int,
) -> bool:
    if df is None or df.empty or len(df) < required_length:
        return False
    opens = [int(x) for x in df["open_time"].tolist()]
    step = TIMEFRAME_SECONDS[timeframe] * 1000
    return all((b - a) == step for a, b in zip(opens, opens[1:]))


def load_candle_cache(history_limit: int) -> dict:
    if not CANDLE_CACHE_FILE.exists():
        return {}
    try:
        with CANDLE_CACHE_FILE.open("rb") as f:
            cache = pickle.load(f)
        if not isinstance(cache, dict):
            return {}
        valid: dict = {}
        for key, df in cache.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                valid[key] = df.tail(history_limit).reset_index(drop=True)
        print(f"[CACHE] {len(valid)} pair 로드")
        return valid
    except Exception as e:
        print(f"[CACHE] 로드 실패: {e}")
        return {}


def save_candle_cache(cache: dict) -> None:
    tmp = CANDLE_CACHE_FILE.with_suffix(".tmp")
    with CACHE_LOCK:
        snapshot = {
            key: df.copy(deep=True) if isinstance(df, pd.DataFrame) else df
            for key, df in cache.items()
        }

    CANDLE_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with tmp.open("wb") as f:
        pickle.dump(snapshot, f, protocol=pickle.HIGHEST_PROTOCOL)
    tmp.replace(CANDLE_CACHE_FILE)


def cache_flush_loop() -> None:
    while not ENGINE_STOP.wait(CACHE_FLUSH_SECONDS):
        if not CACHE_DIRTY.is_set():
            continue
        try:
            CACHE_DIRTY.clear()
            save_candle_cache(CANDLE_CACHE)
        except Exception as e:
            CACHE_DIRTY.set()
            print(f"[CACHE SAVE ERROR] {e}")


# ============================================================
# INDICATOR STATE / TRANSACTION SNAPSHOTS
# ============================================================

def collect_indicator_states_checked() -> tuple[dict[str, dict], bool]:
    states: dict[str, dict] = {}
    with INDICATOR_STATE_LOCK:
        for indicator in INDICATORS:
            try:
                value = indicator.get_state()
                if not isinstance(value, dict):
                    raise TypeError(f"{indicator.name}.get_state() must return dict")
                states[indicator.name] = value
            except Exception as e:
                print(f"[STATE GET ERROR] {indicator.name}: {e}")
                return {}, False
    return states, True


def snapshot_pair_states(symbol: str, timeframe: str) -> list[tuple[BaseIndicator, Any]]:
    snapshots: list[tuple[BaseIndicator, Any]] = []
    for indicator in INDICATORS:
        snapshots.append(
            (indicator, indicator.snapshot_pair_state(symbol, timeframe))
        )
    return snapshots


def restore_pair_states(
    symbol: str,
    timeframe: str,
    snapshots: list[tuple[BaseIndicator, Any]],
) -> None:
    for indicator, snapshot in snapshots:
        try:
            indicator.restore_pair_state(symbol, timeframe, snapshot)
        except Exception as e:
            print(
                f"[CRITICAL][ROLLBACK ERROR] {indicator.name} "
                f"{symbol} {timeframe}: {e}"
            )


def load_indicator_states_from_store() -> None:
    if STORE is None:
        return
    stored = STORE.load_indicator_states()
    for indicator in INDICATORS:
        try:
            state = stored.get(indicator.name, {})
            indicator.load_state(state if isinstance(state, dict) else {})
        except Exception as e:
            print(f"[STATE LOAD ERROR] {indicator.name}: {e}")


def save_state_snapshot() -> None:
    states, ok = collect_indicator_states_checked()
    if not ok:
        raise RuntimeError("indicator state snapshot failed")

    snapshot = {
        "indicators": states,
        "processed_candles": dict(PROCESSED_OPEN_TIME),
        "sqlite_db": str(DB_FILE),
    }
    tmp = STATE_FILE.with_suffix(".tmp")
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp.replace(STATE_FILE)


def state_flush_loop() -> None:
    while not ENGINE_STOP.wait(STATE_FLUSH_SECONDS):
        if not STATE_DIRTY.is_set():
            continue
        try:
            STATE_DIRTY.clear()
            save_state_snapshot()
        except Exception as e:
            STATE_DIRTY.set()
            print(f"[STATE SAVE ERROR] {e}")


# ============================================================
# TELEGRAM / OUTBOX
# ============================================================

_TELEGRAM_LOCAL = threading.local()


def _telegram_session() -> requests.Session:
    session = getattr(_TELEGRAM_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        _TELEGRAM_LOCAL.session = session
    return session


def make_signal_message(signal: Signal) -> str:
    side = str(signal.side).upper()
    icon = "🟢" if side == "LONG" else "🔴" if side == "SHORT" else "🔔"
    title = signal.title or f"{signal.indicator} {side}"

    lines = [
        f"{icon} {title}",
        f"종목: {signal.symbol}",
        f"시간봉: {signal.timeframe}",
    ]

    if signal.price is not None:
        lines.append(f"가격: {fmt_price(float(signal.price))}")

    for key, value in signal.extra.items():
        lines.append(f"{key}: {value}")

    if signal.message:
        lines.append(signal.message)

    if signal.ts_ms is not None:
        dt = datetime.fromtimestamp(
            signal.ts_ms / 1000,
            tz=timezone.utc,
        ).astimezone(KST)
        lines.append(f"감지 시각: {dt:%Y-%m-%d %H:%M:%S.%f} KST")

    return "\n".join(lines)


def make_global_signal_key(signal: Signal, chat_id: str) -> str:
    return (
        f"{chat_id}:{signal.indicator}:{signal.symbol}:"
        f"{signal.timeframe}:{signal.signal_id}"
    )


def prepare_outbox_rows(signals: list[Signal]) -> list[dict]:
    rows: list[dict] = []
    for signal in signals:
        targets = get_telegram_targets_for_symbol(signal.symbol)
        if not targets:
            raise RuntimeError(
                f"No Telegram target: {signal.symbol} "
                f"{signal.indicator} {signal.timeframe}"
            )

        text = make_signal_message(signal)
        for group_name, chat_id in targets:
            rows.append(
                {
                    "signal_key": make_global_signal_key(signal, chat_id),
                    "group_name": group_name,
                    "chat_id": chat_id,
                    "text": text,
                }
            )
    return rows


def send_telegram(chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = _telegram_session().post(
        url,
        data={
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        },
        timeout=REQUEST_TIMEOUT,
    )

    if r.status_code == 429:
        try:
            body = r.json()
            retry_after = float(
                ((body.get("parameters") or {}).get("retry_after")) or 1.0
            )
        except Exception:
            retry_after = 1.0
        raise RuntimeError(f"Telegram 429 retry_after={retry_after}")

    r.raise_for_status()
    body = r.json()
    if not body.get("ok"):
        raise RuntimeError(f"Telegram error: {body}")


def telegram_worker_loop(worker_index: int) -> None:
    print(f"[TELEGRAM WORKER {worker_index}] 시작")
    while not ENGINE_STOP.is_set():
        if STORE is None:
            time.sleep(0.2)
            continue

        try:
            row = STORE.claim_outbox()
            if row is None:
                TELEGRAM_WAKE.wait(TELEGRAM_POLL_SECONDS)
                TELEGRAM_WAKE.clear()
                continue

            start = time.monotonic()
            try:
                send_telegram(str(row["chat_id"]), str(row["text"]))
                STORE.mark_outbox_sent(int(row["id"]))
                elapsed_ms = (time.monotonic() - start) * 1000
                print(
                    f"[ALERT][{row['group_name']}][telegram={elapsed_ms:.0f}ms] "
                    f"{str(row['text']).replace(chr(10), ' | ')}"
                )
            except Exception as e:
                STORE.mark_outbox_retry(
                    int(row["id"]),
                    int(row["attempts"]),
                    str(e),
                )
                print(
                    f"[TELEGRAM RETRY][{row['group_name']}] "
                    f"attempt={row['attempts']} chat_id={row['chat_id']}: {e}"
                )
        except Exception as e:
            print(f"[TELEGRAM WORKER ERROR] worker={worker_index}: {e}")
            time.sleep(0.5)

    print(f"[TELEGRAM WORKER {worker_index}] 종료")


def start_telegram_workers() -> None:
    TELEGRAM_THREADS.clear()
    for i in range(TELEGRAM_WORKERS):
        t = threading.Thread(
            target=telegram_worker_loop,
            args=(i,),
            name=f"TelegramWorker-{i}",
            daemon=True,
        )
        t.start()
        TELEGRAM_THREADS.append(t)


# ============================================================
# LIVE BAR
# ============================================================

def update_live_bar(
    symbol: str,
    timeframe: str,
    price: float,
    ts_ms: int,
) -> LiveBar:
    key = pair_key(symbol, timeframe)
    bucket = timeframe_bucket(timeframe, ts_ms)
    open_ms = bucket_open_ms(timeframe, ts_ms)

    with LIVE_BAR_LOCK:
        bar = LIVE_BARS.get(key)
        if bar is None or bar.bucket != bucket:
            bar = LiveBar(
                bucket=bucket,
                open=float(price),
                high=float(price),
                low=float(price),
                close=float(price),
                first_ts_ms=open_ms,
                last_ts_ms=ts_ms,
            )
            LIVE_BARS[key] = bar
        else:
            bar.high = max(bar.high, float(price))
            bar.low = min(bar.low, float(price))
            # Trade is the most granular source; keep latest trade close.
            if ts_ms >= bar.last_ts_ms:
                bar.close = float(price)
                bar.last_ts_ms = ts_ms

        return copy.copy(bar)


def seed_live_bar_from_current_candle(
    symbol: str,
    timeframe: str,
    current: dict,
) -> LiveBar:
    key = pair_key(symbol, timeframe)
    ts_ms = int(current["open_time"])
    bucket = timeframe_bucket(timeframe, ts_ms)

    with LIVE_BAR_LOCK:
        existing = LIVE_BARS.get(key)

        if existing is None or bucket > existing.bucket:
            bar = LiveBar(
                bucket=bucket,
                open=float(current["open"]),
                high=float(current["high"]),
                low=float(current["low"]),
                close=float(current["close"]),
                first_ts_ms=ts_ms,
                last_ts_ms=int(time.time() * 1000),
            )
            LIVE_BARS[key] = bar
            return copy.copy(bar)

        if bucket == existing.bucket:
            # Preserve trade extremes and use candle open, which is exact.
            existing.open = float(current["open"])
            existing.high = max(existing.high, float(current["high"]))
            existing.low = min(existing.low, float(current["low"]))
            # Do not overwrite a more recent trade close.
            return copy.copy(existing)

        return copy.copy(existing)


def set_live_bar_from_candle(symbol: str, timeframe: str, row: dict) -> LiveBar:
    return seed_live_bar_from_current_candle(symbol, timeframe, row)


def current_live_candle(symbol: str, timeframe: str) -> Optional[dict]:
    key = pair_key(symbol, timeframe)
    with LIVE_BAR_LOCK:
        bar = LIVE_BARS.get(key)
        if bar is None:
            return None
        open_time = int(bar.first_ts_ms)
        return {
            "open_time": open_time,
            "open": float(bar.open),
            "high": float(bar.high),
            "low": float(bar.low),
            "close": float(bar.close),
            "close_time": open_time + TIMEFRAME_SECONDS[timeframe] * 1000,
            "confirm": "0",
        }


# ============================================================
# INDICATOR EXECUTION
# ============================================================

def configure_indicators(indicators: list[BaseIndicator]) -> None:
    global INDICATORS, LIVE_INDICATORS, HISTORY_LIMIT

    if not indicators:
        raise ValueError("등록할 지표가 하나 이상 필요합니다.")

    names = [i.name for i in indicators]
    if len(names) != len(set(names)):
        raise ValueError("indicator.name은 서로 달라야 합니다.")

    if REQUIRE_TRANSACTIONAL_INDICATORS:
        bad = [i.name for i in indicators if not i.transactional_pair_state]
        if bad:
            raise ValueError(
                "상용 모드에서는 snapshot/rollback 지원 지표만 허용합니다: "
                f"{bad}"
            )

    INDICATORS = list(indicators)
    LIVE_INDICATORS = [i for i in INDICATORS if i.use_live_trade]
    HISTORY_LIMIT = max(int(i.history_limit) for i in INDICATORS)


def run_startup_indicators(
    symbol: str,
    timeframe: str,
    df: pd.DataFrame,
    current_candle: Optional[dict],
) -> bool:
    for indicator in INDICATORS:
        try:
            signals = indicator.on_candle(
                symbol=symbol,
                timeframe=timeframe,
                df=df,
                current_candle=current_candle,
                startup=True,
            )
            if signals:
                print(
                    f"[STARTUP SIGNAL SUPPRESSED] {indicator.name} "
                    f"{symbol} {timeframe} count={len(signals)}"
                )
        except Exception as e:
            print(
                f"[INDICATOR ERROR][STARTUP] {indicator.name} "
                f"{symbol} {timeframe}: {e}"
            )
            return False
    return True


def process_trade(symbol: str, price: float, ts_ms: int) -> None:
    if not LIVE_INDICATORS:
        return

    for timeframe in TIMEFRAMES:
        key = pair_key(symbol, timeframe)

        # Drop a delayed trade that belongs to an already committed candle.
        trade_open = bucket_open_ms(timeframe, ts_ms)
        last_processed = PROCESSED_OPEN_TIME.get(key)
        if last_processed is not None and trade_open <= last_processed:
            continue

        live_bar = update_live_bar(symbol, timeframe, price, ts_ms)

        if not is_pair_ready(symbol, timeframe):
            continue
        if is_pair_recovering(symbol, timeframe):
            continue

        snapshots = snapshot_pair_states(symbol, timeframe)
        all_signals: list[Signal] = []

        try:
            for indicator in LIVE_INDICATORS:
                signals = indicator.on_trade(
                    symbol=symbol,
                    timeframe=timeframe,
                    price=price,
                    ts_ms=ts_ms,
                    live_bar=live_bar,
                )
                if signals:
                    all_signals.extend(signals)

            if not all_signals:
                continue

            outbox_rows = prepare_outbox_rows(all_signals)
            states, ok = collect_indicator_states_checked()
            if not ok:
                raise RuntimeError("indicator state snapshot failed")

            if STORE is None:
                raise RuntimeError("DurableStore not initialized")

            STORE.commit_live_signals(
                outbox_rows=outbox_rows,
                indicator_states=states,
            )
            TELEGRAM_WAKE.set()
            STATE_DIRTY.set()

        except Exception as e:
            restore_pair_states(symbol, timeframe, snapshots)
            print(
                f"[LIVE TRANSACTION ROLLBACK] {symbol} {timeframe}: {e}"
            )


def process_closed_candle(
    symbol: str,
    timeframe: str,
    row: dict,
) -> str:
    """
    Returns:
      "ok"       committed or duplicate
      "deferred" gap/recovery/init gate
      "failed"   retryable processing failure
    """
    key = pair_key(symbol, timeframe)
    open_time = int(row["open_time"])

    if not is_pair_ready(symbol, timeframe):
        with EVENT_GATE_LOCK:
            GATED_CANDLE_BUFFER.setdefault(key, {})[open_time] = dict(row)
        return "deferred"

    last = PROCESSED_OPEN_TIME.get(key)
    if last is not None and open_time <= last:
        return "ok"

    if last is not None:
        expected_next = last + TIMEFRAME_SECONDS[timeframe] * 1000
        if open_time > expected_next:
            start_single_pair_recovery(symbol, timeframe, extra_row=row)
            return "deferred"

    snapshots = snapshot_pair_states(symbol, timeframe)

    clean_row = dict(row)
    clean_row.pop("confirm", None)
    new_df = pd.DataFrame([clean_row])

    with CACHE_LOCK:
        old_df = CANDLE_CACHE.get(key)
        merged = merge_candles(old_df, new_df, HISTORY_LIMIT)
        # Cache may move forward before DB commit. This is safe because
        # indicator pair state is rolled back and checkpoint remains old.
        CANDLE_CACHE[key] = merged

    current = current_live_candle(symbol, timeframe)
    if current is not None:
        closed_bucket = timeframe_bucket(timeframe, open_time)
        current_bucket = timeframe_bucket(timeframe, int(current["open_time"]))
        if current_bucket <= closed_bucket:
            current = None

    try:
        all_signals: list[Signal] = []
        for indicator in INDICATORS:
            signals = indicator.on_candle(
                symbol=symbol,
                timeframe=timeframe,
                df=merged,
                current_candle=current,
                startup=False,
            )
            if signals:
                all_signals.extend(signals)

        outbox_rows = prepare_outbox_rows(all_signals)
        states, ok = collect_indicator_states_checked()
        if not ok:
            raise RuntimeError("indicator state snapshot failed")

        if STORE is None:
            raise RuntimeError("DurableStore not initialized")

        STORE.commit_candle_result(
            key=key,
            open_time=open_time,
            outbox_rows=outbox_rows,
            indicator_states=states,
        )

        PROCESSED_OPEN_TIME[key] = open_time
        CACHE_DIRTY.set()
        STATE_DIRTY.set()
        if outbox_rows:
            TELEGRAM_WAKE.set()

        return "ok"

    except Exception as e:
        restore_pair_states(symbol, timeframe, snapshots)
        print(
            f"[CANDLE TRANSACTION ROLLBACK] {symbol} {timeframe} "
            f"open={open_time}: {e}"
        )
        return "failed"


# ============================================================
# EVENT GATE / GAP RECOVERY
# ============================================================

def gate_or_enqueue_closed_candle(
    symbol: str,
    timeframe: str,
    row: dict,
) -> None:
    key = pair_key(symbol, timeframe)
    with EVENT_GATE_LOCK:
        if not is_pair_ready(symbol, timeframe) or key in RECOVERING_PAIRS:
            GATED_CANDLE_BUFFER.setdefault(key, {})[int(row["open_time"])] = dict(row)
            return

        enqueue_realtime_event(
            RealtimeEvent(
                kind="closed_candle",
                symbol=symbol,
                timeframe=timeframe,
                candle=dict(row),
                ts_ms=int(row["close_time"]),
                enqueued_mono=time.monotonic(),
            )
        )


def finish_pair_initialization_and_flush(symbol: str, timeframe: str) -> None:
    key = pair_key(symbol, timeframe)
    with EVENT_GATE_LOCK:
        _mark_pair_ready(key)
        buffered = list(GATED_CANDLE_BUFFER.pop(key, {}).values())
        buffered.sort(key=lambda r: int(r["open_time"]))
        last = PROCESSED_OPEN_TIME.get(key)
        for row in buffered:
            if last is not None and int(row["open_time"]) <= last:
                continue
            enqueue_realtime_event(
                RealtimeEvent(
                    kind="closed_candle",
                    symbol=symbol,
                    timeframe=timeframe,
                    candle=dict(row),
                    ts_ms=int(row["close_time"]),
                    enqueued_mono=time.monotonic(),
                )
            )


def _rows_contiguous_from(
    rows: list[dict],
    timeframe: str,
    last_open: int,
) -> bool:
    if not rows:
        return True
    step = TIMEFRAME_SECONDS[timeframe] * 1000
    expected = last_open + step
    for row in rows:
        ts = int(row["open_time"])
        if ts != expected:
            return False
        expected += step
    return True


def start_single_pair_recovery(
    symbol: str,
    timeframe: str,
    extra_row: Optional[dict] = None,
) -> None:
    key = pair_key(symbol, timeframe)

    with EVENT_GATE_LOCK:
        if extra_row is not None:
            GATED_CANDLE_BUFFER.setdefault(key, {})[
                int(extra_row["open_time"])
            ] = dict(extra_row)

        if key in RECOVERING_PAIRS:
            return
        RECOVERING_PAIRS.add(key)

    threading.Thread(
        target=_recover_one_pair,
        args=(symbol, timeframe),
        name=f"GapRecover-{symbol}-{timeframe}",
        daemon=True,
    ).start()


def _recover_one_pair(symbol: str, timeframe: str) -> None:
    key = pair_key(symbol, timeframe)

    try:
        for attempt in range(GAP_RECOVERY_ATTEMPTS):
            if ENGINE_STOP.is_set():
                return

            last = PROCESSED_OPEN_TIME.get(key)
            if last is None:
                return

            recovered = fetch_closed_since(symbol, timeframe, last)

            with EVENT_GATE_LOCK:
                combined: dict[int, dict] = {
                    int(r["open_time"]): dict(r) for r in recovered
                }
                for r in GATED_CANDLE_BUFFER.get(key, {}).values():
                    combined[int(r["open_time"])] = dict(r)

                rows = [
                    combined[ts]
                    for ts in sorted(combined)
                    if ts > last
                ]

            if _rows_contiguous_from(rows, timeframe, last):
                with EVENT_GATE_LOCK:
                    # Merge any rows that arrived while validating.
                    for r in GATED_CANDLE_BUFFER.pop(key, {}).values():
                        combined[int(r["open_time"])] = dict(r)

                    rows = [
                        combined[ts]
                        for ts in sorted(combined)
                        if ts > last
                    ]

                    if not _rows_contiguous_from(rows, timeframe, last):
                        # Put them back and try again.
                        for r in rows:
                            GATED_CANDLE_BUFFER.setdefault(key, {})[
                                int(r["open_time"])
                            ] = dict(r)
                    else:
                        for r in rows:
                            enqueue_realtime_event(
                                RealtimeEvent(
                                    kind="closed_candle",
                                    symbol=symbol,
                                    timeframe=timeframe,
                                    candle=dict(r),
                                    ts_ms=int(r["close_time"]),
                                    enqueued_mono=time.monotonic(),
                                )
                            )
                        RECOVERING_PAIRS.discard(key)
                        if rows:
                            print(
                                f"[GAP RECOVERED] {symbol} {timeframe} "
                                f"{len(rows)}개"
                            )
                        return

            time.sleep(min(2 ** attempt, 8))

        print(
            f"[CRITICAL][GAP RECOVERY FAILED] {symbol} {timeframe}; "
            f"checkpoint 유지, 다음 이벤트에서 재시도"
        )

    except Exception as e:
        print(f"[GAP RECOVERY ERROR] {symbol} {timeframe}: {e}")

    finally:
        with EVENT_GATE_LOCK:
            RECOVERING_PAIRS.discard(key)


# ============================================================
# SEPARATE REALTIME QUEUES
# ============================================================

def enqueue_realtime_event(event: RealtimeEvent) -> None:
    idx = stable_worker_index(event.symbol)
    q = CANDLE_QUEUES[idx] if event.kind == "closed_candle" else TRADE_QUEUES[idx]
    q.put_nowait(event)

    size = q.qsize()
    if size >= REALTIME_QUEUE_WARN_SIZE and size % 1000 == 0:
        print(
            f"[CRITICAL][REALTIME BACKLOG] worker={idx} "
            f"kind={event.kind} queue={size} symbol={event.symbol}"
        )


def schedule_candle_retry(event: RealtimeEvent) -> None:
    if event.attempt >= CANDLE_PROCESS_MAX_IMMEDIATE_RETRIES:
        print(
            f"[CRITICAL][CANDLE RETRY LIMIT] {event.symbol} {event.timeframe} "
            f"open={event.candle.get('open_time') if event.candle else None}. "
            f"checkpoint는 전진하지 않았습니다."
        )
        return

    def retry() -> None:
        if ENGINE_STOP.wait(CANDLE_PROCESS_RETRY_SECONDS):
            return
        retry_event = copy.deepcopy(event)
        retry_event.attempt += 1
        retry_event.enqueued_mono = time.monotonic()
        enqueue_realtime_event(retry_event)

    threading.Thread(
        target=retry,
        name=f"CandleRetry-{event.symbol}-{event.timeframe}",
        daemon=True,
    ).start()


def _get_next_event(worker_index: int) -> tuple[Optional[RealtimeEvent], Optional[Queue]]:
    cq = CANDLE_QUEUES[worker_index]
    tq = TRADE_QUEUES[worker_index]

    # Hard priority for closed candles.
    try:
        return cq.get_nowait(), cq
    except Empty:
        pass

    # Short wait for trade. Re-check candle before processing trade.
    try:
        trade = tq.get(timeout=0.05)
    except Empty:
        return None, None

    try:
        candle = cq.get_nowait()
        # Put trade back at the front is not possible with Queue.
        # Requeueing at tail is acceptable because stale trades are discarded
        # after a closed-candle checkpoint.
        tq.task_done()
        tq.put_nowait(trade)
        return candle, cq
    except Empty:
        return trade, tq


def realtime_worker_loop(worker_index: int) -> None:
    print(f"[REALTIME WORKER {worker_index}] 시작")

    while not ENGINE_STOP.is_set():
        event, source_q = _get_next_event(worker_index)
        if event is None or source_q is None:
            continue

        try:
            lag_ms = (time.monotonic() - event.enqueued_mono) * 1000
            if lag_ms >= REALTIME_LAG_WARN_MS:
                print(
                    f"[CRITICAL][REALTIME LAG] worker={worker_index} "
                    f"lag={lag_ms:.0f}ms kind={event.kind} symbol={event.symbol}"
                )

            if event.kind == "trade":
                if event.price is not None and event.ts_ms is not None:
                    process_trade(event.symbol, event.price, event.ts_ms)

            elif event.kind == "closed_candle":
                if event.timeframe and event.candle:
                    status = process_closed_candle(
                        event.symbol,
                        event.timeframe,
                        event.candle,
                    )
                    if status == "failed":
                        schedule_candle_retry(event)

        except Exception as e:
            print(
                f"[REALTIME WORKER ERROR] worker={worker_index} "
                f"kind={event.kind} symbol={event.symbol}: {e}"
            )
            if event.kind == "closed_candle" and event.candle and event.timeframe:
                schedule_candle_retry(event)
        finally:
            source_q.task_done()

    print(f"[REALTIME WORKER {worker_index}] 종료")


def start_realtime_workers() -> None:
    REALTIME_THREADS.clear()
    for i in range(REALTIME_WORKERS):
        t = threading.Thread(
            target=realtime_worker_loop,
            args=(i,),
            name=f"RealtimeWorker-{i}",
            daemon=True,
        )
        t.start()
        REALTIME_THREADS.append(t)


# ============================================================
# OKX TRADE WS
# ============================================================

class OKXTradeWebSocket:
    def __init__(self, symbols: list[str], name: str):
        self.symbols = list(symbols)
        self.name = name
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.ws = None

    def start(self) -> None:
        if not LIVE_INDICATORS or not self.symbols:
            return
        self.thread = threading.Thread(
            target=self._run_loop,
            name=self.name,
            daemon=True,
        )
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        try:
            if self.ws is not None:
                self.ws.close()
        except Exception:
            pass

    def _on_open(self, ws) -> None:
        print(f"[{self.name}] 연결됨 ({len(self.symbols)}종목)")
        args = [{"channel": "trades", "instId": s} for s in self.symbols]
        for i in range(0, len(args), WS_SUBSCRIBE_CHUNK):
            ws.send(
                json.dumps(
                    {"op": "subscribe", "args": args[i:i + WS_SUBSCRIBE_CHUNK]}
                )
            )
            time.sleep(0.10)

    def _on_message(self, ws, message) -> None:
        if message == "pong":
            return
        try:
            payload = json.loads(message)
        except Exception:
            return

        if "event" in payload:
            if payload.get("event") == "error":
                print(f"[{self.name} ERROR EVENT] {payload}")
            return

        arg = payload.get("arg") or {}
        if arg.get("channel") != "trades":
            return

        for trade in payload.get("data") or []:
            try:
                symbol = str(trade["instId"])
                price = float(trade["px"])
                ts_ms = int(trade["ts"])
            except Exception:
                continue

            enqueue_realtime_event(
                RealtimeEvent(
                    kind="trade",
                    symbol=symbol,
                    price=price,
                    ts_ms=ts_ms,
                    enqueued_mono=time.monotonic(),
                )
            )

    def _on_error(self, ws, error) -> None:
        if not self.stop_event.is_set():
            print(f"[{self.name} ERROR] {error}")

    def _on_close(self, ws, status_code, msg) -> None:
        if not self.stop_event.is_set():
            print(f"[{self.name} CLOSED] code={status_code} msg={msg}")

    def _run_loop(self) -> None:
        while not self.stop_event.is_set() and not ENGINE_STOP.is_set():
            try:
                WS_CONNECT_LIMITER.wait()
                self.ws = websocket.WebSocketApp(
                    OKX_PUBLIC_WS,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                if not self.stop_event.is_set():
                    print(f"[{self.name} LOOP ERROR] {e}")

            if not self.stop_event.is_set() and not ENGINE_STOP.is_set():
                time.sleep(WS_RECONNECT_SECONDS + random.uniform(0.0, 1.5))


# ============================================================
# OKX CANDLE WS
# ============================================================

class OKXCandleWebSocket:
    def __init__(self, pairs: list[tuple[str, str]], name: str):
        self.pairs = list(pairs)
        self.name = name
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.ws = None
        self.open_count = 0

    def start(self) -> None:
        if not self.pairs:
            return
        self.thread = threading.Thread(
            target=self._run_loop,
            name=self.name,
            daemon=True,
        )
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        try:
            if self.ws is not None:
                self.ws.close()
        except Exception:
            pass

    def _begin_reconnect_recovery(self) -> None:
        now_ms = int(time.time() * 1000)
        to_recover: list[tuple[str, str]] = []

        with EVENT_GATE_LOCK:
            for symbol, timeframe in self.pairs:
                if not is_pair_ready(symbol, timeframe):
                    continue
                key = pair_key(symbol, timeframe)
                last = PROCESSED_OPEN_TIME.get(key)
                expected = expected_last_closed_open_ms(timeframe, now_ms)
                if last is None or last >= expected or key in RECOVERING_PAIRS:
                    continue
                RECOVERING_PAIRS.add(key)
                to_recover.append((symbol, timeframe))

        if not to_recover:
            return

        print(f"[{self.name}] 재연결 GAP 복구 대상={len(to_recover)}")

        def batch() -> None:
            with ThreadPoolExecutor(max_workers=RECOVERY_MAX_WORKERS) as executor:
                futures = [
                    executor.submit(_recover_one_pair, s, tf)
                    for s, tf in to_recover
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"[RECONNECT RECOVERY ERROR] {e}")

        threading.Thread(
            target=batch,
            name=f"{self.name}-Recovery",
            daemon=True,
        ).start()

    def _on_open(self, ws) -> None:
        self.open_count += 1
        print(f"[{self.name}] 연결됨 ({len(self.pairs)} subscriptions)")

        args = [
            {"channel": OKX_CANDLE_CHANNEL[tf], "instId": symbol}
            for symbol, tf in self.pairs
        ]
        for i in range(0, len(args), WS_SUBSCRIBE_CHUNK):
            ws.send(
                json.dumps(
                    {"op": "subscribe", "args": args[i:i + WS_SUBSCRIBE_CHUNK]}
                )
            )
            time.sleep(0.10)

        if self.open_count > 1:
            self._begin_reconnect_recovery()

    def _on_message(self, ws, message) -> None:
        if message == "pong":
            return
        try:
            payload = json.loads(message)
        except Exception:
            return

        if "event" in payload:
            if payload.get("event") == "error":
                print(f"[{self.name} ERROR EVENT] {payload}")
            return

        arg = payload.get("arg") or {}
        channel = str(arg.get("channel") or "")
        symbol = str(arg.get("instId") or "")
        if not channel.startswith("candle") or not symbol:
            return

        timeframe = next(
            (tf for tf, ch in OKX_CANDLE_CHANNEL.items() if ch == channel),
            None,
        )
        if timeframe is None:
            return

        for k in payload.get("data") or []:
            row = candle_row(k, timeframe)
            if row is None:
                continue

            if row["confirm"] != "1":
                set_live_bar_from_candle(symbol, timeframe, row)
            else:
                gate_or_enqueue_closed_candle(symbol, timeframe, row)

    def _on_error(self, ws, error) -> None:
        if not self.stop_event.is_set():
            print(f"[{self.name} ERROR] {error}")

    def _on_close(self, ws, status_code, msg) -> None:
        if not self.stop_event.is_set():
            print(f"[{self.name} CLOSED] code={status_code} msg={msg}")

    def _run_loop(self) -> None:
        while not self.stop_event.is_set() and not ENGINE_STOP.is_set():
            try:
                WS_CONNECT_LIMITER.wait()
                self.ws = websocket.WebSocketApp(
                    OKX_BUSINESS_WS,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                if not self.stop_event.is_set():
                    print(f"[{self.name} LOOP ERROR] {e}")

            if not self.stop_event.is_set() and not ENGINE_STOP.is_set():
                time.sleep(WS_RECONNECT_SECONDS + random.uniform(0.0, 1.5))


# ============================================================
# INITIALIZATION
# ============================================================

def validate_settings() -> None:
    if not RUNTIME_CONFIGURED:
        raise RuntimeError("configure_runtime()을 먼저 호출해야 합니다.")

    bad = [
        tf for tf in TIMEFRAMES
        if tf not in OKX_INTERVAL
        or tf not in OKX_CANDLE_CHANNEL
        or tf not in TIMEFRAME_SECONDS
    ]
    if bad:
        raise ValueError(f"지원하지 않는 timeframe: {bad}")

    if not INDICATORS:
        raise ValueError("INDICATORS가 비어 있습니다.")

    # Production fail-fast: no silent alert loss.
    if not TELEGRAM_BOT_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN이 비어 있습니다.")

    for group in SYMBOL_GROUPS:
        if not str(group["chat_id"]).strip():
            raise ValueError(f"{group['name']} Telegram chat_id가 비어 있습니다.")


def _initial_history_for_pair(
    symbol: str,
    timeframe: str,
) -> tuple[pd.DataFrame, Optional[dict]]:
    key = pair_key(symbol, timeframe)

    with CACHE_LOCK:
        cached = CANDLE_CACHE.get(key)
        cached_df = (
            cached.copy(deep=True)
            if isinstance(cached, pd.DataFrame)
            else None
        )

    expected = expected_last_closed_open_ms(timeframe)
    cache_ok = (
        cache_is_contiguous(cached_df, timeframe, HISTORY_LIMIT)
        and int(cached_df.iloc[-1]["open_time"]) >= expected
    )

    if cache_ok:
        recent_df, current = fetch_recent_bundle(symbol, timeframe)
        merged = merge_candles(cached_df, recent_df, HISTORY_LIMIT)
    else:
        history_df = fetch_closed_klines(symbol, timeframe, HISTORY_LIMIT)
        recent_df, current = fetch_recent_bundle(symbol, timeframe)
        merged = merge_candles(history_df, recent_df, HISTORY_LIMIT)

    with EVENT_GATE_LOCK:
        buffered = list(GATED_CANDLE_BUFFER.get(key, {}).values())

    if buffered:
        buffered_df = pd.DataFrame(
            [{k: v for k, v in row.items() if k != "confirm"} for row in buffered]
        )
        merged = merge_candles(merged, buffered_df, HISTORY_LIMIT)

    with CACHE_LOCK:
        CANDLE_CACHE[key] = merged

    CACHE_DIRTY.set()
    return merged, current


def initialize_pair(symbol: str, timeframe: str) -> None:
    key = pair_key(symbol, timeframe)
    _mark_pair_not_ready(key)

    df, current = _initial_history_for_pair(symbol, timeframe)

    if current is not None:
        seed_live_bar_from_current_candle(symbol, timeframe, current)

    snapshots = snapshot_pair_states(symbol, timeframe)

    try:
        if not run_startup_indicators(symbol, timeframe, df, current):
            raise RuntimeError("startup indicator failed")

        if df.empty:
            raise RuntimeError("empty history")

        last_open = int(df.iloc[-1]["open_time"])

        states, ok = collect_indicator_states_checked()
        if not ok:
            raise RuntimeError("indicator state snapshot failed")

        if STORE is None:
            raise RuntimeError("DurableStore not initialized")

        # No startup Telegram; only state + checkpoint.
        STORE.commit_candle_result(
            key=key,
            open_time=last_open,
            outbox_rows=[],
            indicator_states=states,
        )
        PROCESSED_OPEN_TIME[key] = last_open
        finish_pair_initialization_and_flush(symbol, timeframe)
        print(f"[PAIR READY] {symbol} {timeframe}")
    except Exception:
        restore_pair_states(symbol, timeframe, snapshots)
        raise


def initialize_all_pairs() -> None:
    pairs = [
        (symbol, timeframe)
        for symbol in ALL_SYMBOLS
        for timeframe in TIMEFRAMES
    ]

    priority: dict[str, int] = {}
    for group in SYMBOL_GROUPS:
        p = int(group.get("priority", 99))
        for symbol in group["symbols"]:
            priority[symbol] = min(priority.get(symbol, 99), p)

    pairs.sort(
        key=lambda x: (
            priority.get(x[0], 99),
            x[0],
            TIMEFRAMES.index(x[1]),
        )
    )

    total = len(pairs)
    failed = 0
    print(f"[INIT] startup replay pair={total}")

    with ThreadPoolExecutor(
        max_workers=min(INIT_MAX_WORKERS, MAX_WORKERS)
    ) as executor:
        futures = {
            executor.submit(initialize_pair, s, tf): (s, tf)
            for s, tf in pairs
        }

        completed = 0
        for future in as_completed(futures):
            s, tf = futures[future]
            try:
                future.result()
            except Exception as e:
                failed += 1
                print(f"[ERROR][INIT] {s} {tf}: {e}")

            completed += 1
            if completed % 20 == 0 or completed == total:
                print(
                    f"[INIT PROGRESS] {completed}/{total} failed={failed}"
                )

    if failed:
        print(
            f"[CRITICAL] READY 실패 pair={failed}. "
            f"이 pair들은 신호를 계산하지 않습니다."
        )


# ============================================================
# START / STOP
# ============================================================

def print_indicator_summary() -> None:
    print("[INDICATORS]")
    for indicator in INDICATORS:
        mode = "CANDLE + LIVE" if indicator.use_live_trade else "CANDLE"
        print(
            f"  - {indicator.name} | {mode} | "
            f"history={indicator.history_limit} | transactional={indicator.transactional_pair_state}"
        )


def start_ws_clients() -> tuple[list[OKXTradeWebSocket], list[OKXCandleWebSocket]]:
    trade_clients: list[OKXTradeWebSocket] = []
    candle_clients: list[OKXCandleWebSocket] = []

    if LIVE_INDICATORS:
        for i, shard in enumerate(
            split_evenly(ALL_SYMBOLS, TRADE_WS_CONNECTIONS),
            start=1,
        ):
            client = OKXTradeWebSocket(shard, f"TRADE-WS-{i}")
            client.start()
            trade_clients.append(client)
            time.sleep(WS_CONNECTION_START_GAP)

    pairs = [
        (symbol, timeframe)
        for symbol in ALL_SYMBOLS
        for timeframe in TIMEFRAMES
    ]
    for i, shard in enumerate(
        split_evenly(pairs, CANDLE_WS_CONNECTIONS),
        start=1,
    ):
        client = OKXCandleWebSocket(shard, f"CANDLE-WS-{i}")
        client.start()
        candle_clients.append(client)
        time.sleep(WS_CONNECTION_START_GAP)

    return trade_clients, candle_clients


def stop_ws_clients(
    trade_clients: list[OKXTradeWebSocket],
    candle_clients: list[OKXCandleWebSocket],
) -> None:
    for client in trade_clients:
        client.stop()
    for client in candle_clients:
        client.stop()


def wait_realtime_queues(timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        unfinished = sum(q.unfinished_tasks for q in CANDLE_QUEUES)
        unfinished += sum(q.unfinished_tasks for q in TRADE_QUEUES)
        if unfinished == 0:
            return
        time.sleep(0.05)

    unfinished = sum(q.unfinished_tasks for q in CANDLE_QUEUES)
    unfinished += sum(q.unfinished_tasks for q in TRADE_QUEUES)
    if unfinished:
        print(
            f"[STOP WARN] 미처리 realtime event={unfinished}. "
            f"미checkpoint 확정봉은 재시작 시 복구됩니다."
        )


def main() -> None:
    global CANDLE_CACHE, STORE, LAST_CLOSED_OPEN_TIME

    validate_settings()
    resolve_runtime_symbols()

    if not ALL_SYMBOLS:
        raise ValueError("감시할 OKX SWAP 종목이 없습니다.")

    STORE = DurableStore(DB_FILE)

    PROCESSED_OPEN_TIME.clear()
    PROCESSED_OPEN_TIME.update(STORE.load_processed())
    LAST_CLOSED_OPEN_TIME = PROCESSED_OPEN_TIME

    load_indicator_states_from_store()
    CANDLE_CACHE = load_candle_cache(HISTORY_LIMIT)

    for indicator in INDICATORS:
        indicator.startup()

    print("=" * 78)
    print("Realtime Multi Indicator Engine PRO (OKX SWAP) -> Telegram")
    print(f"전체 감시 종목={len(ALL_SYMBOLS)}")
    for group in SYMBOL_GROUPS:
        print(f"{group['name']}={len(group['symbols'])}")
    print(f"TIMEFRAMES={TIMEFRAMES}")
    print(f"DB={DB_FILE}")
    print_indicator_summary()
    print("=" * 78)

    start_realtime_workers()
    start_telegram_workers()

    threading.Thread(
        target=cache_flush_loop,
        name="CacheFlush",
        daemon=True,
    ).start()

    threading.Thread(
        target=state_flush_loop,
        name="StateFlush",
        daemon=True,
    ).start()

    trade_clients, candle_clients = start_ws_clients()
    print("[READY] WS 수신 시작. 초기화 전 확정봉은 buffer에 보존.")

    initialize_all_pairs()

    try:
        save_candle_cache(CANDLE_CACHE)
        save_state_snapshot()
    except Exception as e:
        print(f"[INITIAL SNAPSHOT ERROR] {e}")

    pruned = STORE.prune_sent_outbox(90)
    if pruned:
        print(f"[OUTBOX] 오래된 SENT {pruned}개 정리")

    print(
        f"[READY] pair={len(READY_PAIRS)}/{len(ALL_SYMBOLS) * len(TIMEFRAMES)}"
    )
    print(
        "[READY] candle 우선 queue / WS GAP 복구 / SQLite outbox / "
        "pair-state rollback 활성"
    )

    try:
        while True:
            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\n[STOP] 종료 처리")

    finally:
        stop_ws_clients(trade_clients, candle_clients)
        wait_realtime_queues(SHUTDOWN_DRAIN_SECONDS)
        ENGINE_STOP.set()
        TELEGRAM_WAKE.set()

        for indicator in INDICATORS:
            try:
                indicator.shutdown()
            except Exception as e:
                print(f"[INDICATOR STOP ERROR] {indicator.name}: {e}")

        try:
            states, ok = collect_indicator_states_checked()
            if ok and STORE is not None:
                STORE.save_indicator_states(states)
        except Exception as e:
            print(f"[DB STATE SAVE ERROR] {e}")

        try:
            save_state_snapshot()
        except Exception as e:
            print(f"[STATE SAVE ERROR] {e}")

        try:
            save_candle_cache(CANDLE_CACHE)
        except Exception as e:
            print(f"[CACHE SAVE ERROR] {e}")

        if STORE is not None:
            pending = STORE.pending_outbox_count()
            if pending:
                print(
                    f"[STOP] Telegram 미전송 {pending}개 DB 보존. "
                    f"다음 실행에서 재전송."
                )
            STORE.close()

        print("[STOPPED]")
