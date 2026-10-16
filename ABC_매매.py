from __future__ import annotations

import copy
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from common_engine import (
    BaseIndicator,
    LiveBar,
    Signal,
    TIMEFRAME_SECONDS,
    configure_indicators,
    configure_runtime,
    make_common_symbol_groups,
    main,
)


# ============================================================
# ABC TELEGRAM CHAT IDS
# ============================================================
# Telegram Bot Token은 common_engine.py의 TELEGRAM_BOT_TOKEN 하나를 공통 사용한다.

ABC_MAJOR_CHAT_ID = "-5113269084"   # 메인코인 방
ABC_MEME_CHAT_ID = "-5399297130"    # 밈코인 방
ABC_ALT_CHAT_ID = "-5283048931"     # 알트코인 방


# ============================================================
# SYMBOL GROUPS
# ============================================================
# 종목 분류 자체는 common_engine.py에서 공통 관리한다.
# 이 지표 파일은 ABC 전용 Telegram 방만 지정한다.

EXCLUDE_SYMBOLS: set[str] = set()

SYMBOL_GROUPS = make_common_symbol_groups(
    major_chat_id=ABC_MAJOR_CHAT_ID,
    meme_chat_id=ABC_MEME_CHAT_ID,
    alt_chat_id=ABC_ALT_CHAT_ID,
)


# ============================================================
# TIMEFRAMES
# ============================================================

TIMEFRAMES = ["5m", "15m", "1h", "4h", "1d"]


# ============================================================
# PINE SETTINGS
# ============================================================

LEFT_BARS = 5
RIGHT_BARS = 3

TOLERANCE = 0.10
MIN_MOVE = 3.0
MAX_AGE = 150
BREAK_MARGIN = 0.05

RATIOS = (1.0, 1.13, 1.272)

# maxAge 150보다 충분히 크게.
HISTORY_LIMIT = 400

# Pine의 upSetup/downSetup alertcondition까지 Telegram으로 보낼지 여부.
# 원본의 alert()는 target touch에만 있으므로 기본 False.
SEND_SETUP_TELEGRAM = False


# ============================================================
# PAIR STATE
# ============================================================

@dataclass
class PairState:
    prices: list[float] = field(default_factory=list)
    kinds: list[int] = field(default_factory=list)
    pivot_bars: list[int] = field(default_factory=list)

    # slot 0 = 상승 ABC -> SHORT
    # slot 1 = 하락 ABC -> LONG
    active: list[bool] = field(default_factory=lambda: [False, False])
    origins: list[Optional[float]] = field(default_factory=lambda: [None, None])
    lengths: list[Optional[float]] = field(default_factory=lambda: [None, None])
    born: list[int] = field(default_factory=lambda: [0, 0])
    born_open_time: list[int] = field(default_factory=lambda: [0, 0])

    # slot 0: indices 0,1,2
    # slot 1: indices 3,4,5
    targets: list[Optional[float]] = field(default_factory=lambda: [None] * 6)

    # Pine varip used
    used: list[bool] = field(default_factory=lambda: [False] * 6)

    last_closed_index: int = -1
    last_closed_open_time: Optional[int] = None
    initialized: bool = False


# ============================================================
# GORILLA ABC
# ============================================================

class GorillaABCIndicator(BaseIndicator):
    name = "gorilla_abc"
    display_name = "Gorilla ABC · 즉시 터치 시그널"

    use_live_trade = True
    history_limit = HISTORY_LIMIT

    # common_engine PRO가 DB 실패 시 pair state를 rollback할 수 있게 한다.
    transactional_pair_state = True

    def __init__(self) -> None:
        self._states: dict[str, PairState] = {}

        # 전체 지표를 하나의 lock으로 막지 않는다.
        # 각 symbol:timeframe만 독립적으로 직렬화.
        self._pair_locks: dict[str, threading.RLock] = {}
        self._locks_guard = threading.RLock()

        # Low-latency interest registry.
        # Only pairs that still have at least one unused live target receive
        # trade processing from common_engine.
        self._interest_lock = threading.RLock()
        self._live_pairs: set[str] = set()
        self._live_symbol_counts: dict[str, int] = {}

    @staticmethod
    def _key(symbol: str, timeframe: str) -> str:
        return f"{symbol}:{timeframe}"

    def _pair_lock(self, symbol: str, timeframe: str) -> threading.RLock:
        key = self._key(symbol, timeframe)
        with self._locks_guard:
            lock = self._pair_locks.get(key)
            if lock is None:
                lock = threading.RLock()
                self._pair_locks[key] = lock
            return lock

    def _sync_live_interest(
        self,
        symbol: str,
        timeframe: str,
        state: Optional[PairState],
    ) -> None:
        key = self._key(symbol, timeframe)

        needed = False
        if state is not None and state.initialized:
            for slot in range(2):
                if not state.active[slot]:
                    continue

                start = slot * 3
                for k in range(start, start + 3):
                    if state.targets[k] is not None and not state.used[k]:
                        needed = True
                        break

                if needed:
                    break

        with self._interest_lock:
            already = key in self._live_pairs
            if needed == already:
                return

            if needed:
                self._live_pairs.add(key)
                self._live_symbol_counts[symbol] = (
                    self._live_symbol_counts.get(symbol, 0) + 1
                )
            else:
                self._live_pairs.discard(key)
                count = self._live_symbol_counts.get(symbol, 0) - 1
                if count > 0:
                    self._live_symbol_counts[symbol] = count
                else:
                    self._live_symbol_counts.pop(symbol, None)

    def wants_live_trade(self, symbol: str, timeframe: str) -> bool:
        key = self._key(symbol, timeframe)
        with self._interest_lock:
            return key in self._live_pairs

    def wants_symbol_trade(self, symbol: str) -> bool:
        with self._interest_lock:
            return self._live_symbol_counts.get(symbol, 0) > 0

    # ========================================================
    # TRANSACTION SNAPSHOT / ROLLBACK
    # ========================================================

    def snapshot_pair_state(self, symbol: str, timeframe: str) -> Any:
        key = self._key(symbol, timeframe)
        lock = self._pair_lock(symbol, timeframe)
        with lock:
            state = self._states.get(key)
            return None if state is None else copy.deepcopy(state)

    def restore_pair_state(
        self,
        symbol: str,
        timeframe: str,
        snapshot: Any,
    ) -> None:
        key = self._key(symbol, timeframe)
        lock = self._pair_lock(symbol, timeframe)
        with lock:
            if snapshot is None:
                self._states.pop(key, None)
                restored = None
            else:
                self._states[key] = copy.deepcopy(snapshot)
                restored = self._states[key]

            self._sync_live_interest(
                symbol,
                timeframe,
                restored,
            )

    # ========================================================
    # PINE f_pivot()
    # ========================================================

    @staticmethod
    def _pivot_update(
        state: PairState,
        price: float,
        kind: int,
        pivot_bar: int,
    ) -> bool:
        n = len(state.prices)

        if n == 0:
            state.prices.append(float(price))
            state.kinds.append(int(kind))
            state.pivot_bars.append(int(pivot_bar))
            return True

        last = n - 1

        if pivot_bar <= state.pivot_bars[last]:
            return False

        if kind == state.kinds[last]:
            old = state.prices[last]
            more_extreme = (
                (kind == 1 and price > old)
                or
                (kind == -1 and price < old)
            )

            if not more_extreme:
                return False

            state.prices[last] = float(price)
            state.pivot_bars[last] = int(pivot_bar)
            return True

        state.prices.append(float(price))
        state.kinds.append(int(kind))
        state.pivot_bars.append(int(pivot_bar))

        if len(state.prices) > 3:
            state.prices.pop(0)
            state.kinds.pop(0)
            state.pivot_bars.pop(0)

        return True

    # ========================================================
    # ta.pivothigh / ta.pivotlow equivalent
    # ========================================================

    @staticmethod
    def _pivot_at_confirmation(
        highs: list[float],
        lows: list[float],
        confirm_index: int,
    ) -> tuple[Optional[float], Optional[float], Optional[int]]:
        pivot_index = confirm_index - RIGHT_BARS

        if pivot_index < LEFT_BARS:
            return None, None, None

        left_start = pivot_index - LEFT_BARS
        right_end = pivot_index + RIGHT_BARS

        if right_end > confirm_index:
            return None, None, None

        candidate_high = float(highs[pivot_index])
        candidate_low = float(lows[pivot_index])

        left_highs = highs[left_start:pivot_index]
        right_highs = highs[pivot_index + 1:right_end + 1]

        left_lows = lows[left_start:pivot_index]
        right_lows = lows[pivot_index + 1:right_end + 1]

        # Strict comparison:
        # equal neighbouring high/low disqualifies the pivot.
        is_ph = (
            all(candidate_high > float(v) for v in left_highs)
            and
            all(candidate_high > float(v) for v in right_highs)
        )

        is_pl = (
            all(candidate_low < float(v) for v in left_lows)
            and
            all(candidate_low < float(v) for v in right_lows)
        )

        return (
            candidate_high if is_ph else None,
            candidate_low if is_pl else None,
            pivot_index,
        )

    # ========================================================
    # TARGET TOUCH
    # ========================================================

    def _touch_targets(
        self,
        *,
        state: PairState,
        symbol: str,
        timeframe: str,
        bar_index: int,
        low: float,
        high: float,
        ts_ms: int,
        emit: bool,
    ) -> list[Signal]:
        signals: list[Signal] = []

        for slot in range(2):
            if not state.active[slot]:
                continue

            upward = slot == 0
            start_bar = state.born[slot]

            eligible = (
                bar_index > start_bar
                and
                bar_index - start_bar <= MAX_AGE
            )

            if not eligible:
                continue

            for j, ratio in enumerate(RATIOS):
                k = slot * 3 + j
                level = state.targets[k]

                if level is None:
                    continue

                hit = float(low) <= float(level) <= float(high)

                if not hit or state.used[k]:
                    continue

                # Exactly once in indicator memory.
                # If DB transaction fails, common_engine restores the snapshot.
                state.used[k] = True

                if not emit:
                    continue

                side = "SHORT" if upward else "LONG"
                born_open = state.born_open_time[slot]

                signals.append(
                    Signal(
                        indicator=self.name,
                        signal_id=f"{side}:{born_open}:{ratio:.3f}",
                        symbol=symbol,
                        timeframe=timeframe,
                        side=side,
                        price=float(level),
                        title=f"ABC {side} {ratio:g}",
                        message=f"ABC 확장 {ratio:g} 최초 터치",
                        extra={
                            "확장비율": f"{ratio:g}",
                            "목표가": f"{level:.10g}",
                        },
                        ts_ms=int(ts_ms),
                    )
                )

        return signals

    # ========================================================
    # INVALIDATION
    # ========================================================

    @staticmethod
    def _invalidate_patterns(
        state: PairState,
        *,
        bar_index: int,
        high: float,
        low: float,
        close: float,
    ) -> None:
        for slot in range(2):
            if not state.active[slot]:
                continue

            upward = slot == 0
            origin = state.origins[slot]
            wave = state.lengths[slot]
            outer = state.targets[slot * 3 + 2]

            if origin is None or wave is None or outer is None:
                state.active[slot] = False
                continue

            broken_origin = (
                float(low) < float(origin)
                if upward
                else float(high) > float(origin)
            )

            broken_outer = (
                float(close) > float(outer) + float(wave) * BREAK_MARGIN
                if upward
                else float(close) < float(outer) - float(wave) * BREAK_MARGIN
            )

            expired = bar_index - state.born[slot] > MAX_AGE

            if broken_origin or broken_outer or expired:
                state.active[slot] = False

    # ========================================================
    # PATTERN CREATE
    # ========================================================

    def _try_create_pattern(
        self,
        *,
        state: PairState,
        symbol: str,
        timeframe: str,
        bar_index: int,
        open_time: int,
        close: float,
        changed: bool,
        log_setup: bool,
    ) -> list[Signal]:
        if not changed or len(state.prices) != 3:
            return []

        p0, p1, p2 = state.prices

        upward = (
            state.kinds[0] == -1
            and
            state.kinds[1] == 1
        )

        wave = abs(p1 - p0)
        retrace = abs(p1 - p2) / wave if wave > 0 else None

        inside = (
            (p2 > p0 and p2 < p1)
            if upward
            else
            (p2 < p0 and p2 > p1)
        )

        reacted = (
            float(close) > p2
            if upward
            else float(close) < p2
        )

        move_ok = p0 != 0 and (wave / p0 * 100.0 >= MIN_MOVE)

        match = (
            inside
            and reacted
            and retrace is not None
            and abs(retrace - 0.786) <= TOLERANCE
            and move_ok
        )

        if not match:
            return []

        slot = 0 if upward else 1
        direction = 1.0 if upward else -1.0

        state.active[slot] = True
        state.origins[slot] = float(p2)
        state.lengths[slot] = float(wave)
        state.born[slot] = int(bar_index)
        state.born_open_time[slot] = int(open_time)

        for j, ratio in enumerate(RATIOS):
            k = slot * 3 + j
            state.targets[k] = float(p2 + direction * wave * ratio)
            state.used[k] = False

        side = "SHORT" if upward else "LONG"

        if log_setup:
            print(
                f"[ABC SETUP] {symbol} {timeframe} {side} "
                f"| p0={p0:.10g} p1={p1:.10g} p2={p2:.10g}"
            )

        if not SEND_SETUP_TELEGRAM:
            return []

        return [
            Signal(
                indicator=self.name,
                signal_id=f"SETUP:{side}:{open_time}",
                symbol=symbol,
                timeframe=timeframe,
                side=side,
                price=float(p2),
                title=f"ABC {side} 패턴 확인",
                message="0.786 부근 되돌림 ABC 패턴 확인",
                extra={
                    "P0": f"{p0:.10g}",
                    "P1": f"{p1:.10g}",
                    "P2": f"{p2:.10g}",
                },
                ts_ms=int(open_time),
            )
        ]

    # ========================================================
    # HISTORY REPLAY
    # ========================================================

    def _process_history_bar(
        self,
        *,
        state: PairState,
        symbol: str,
        timeframe: str,
        rows: pd.DataFrame,
        highs: list[float],
        lows: list[float],
        index: int,
    ) -> None:
        row = rows.iloc[index]
        bar_index = int(index)
        open_time = int(row["open_time"])

        # Pine execution order:
        # 1) target touch
        # 2) invalidation
        # 3) pivot confirmation
        # 4) new pattern
        self._touch_targets(
            state=state,
            symbol=symbol,
            timeframe=timeframe,
            bar_index=bar_index,
            low=float(row["low"]),
            high=float(row["high"]),
            ts_ms=int(row["close_time"]),
            emit=False,
        )

        self._invalidate_patterns(
            state,
            bar_index=bar_index,
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
        )

        ph, pl, pivot_index = self._pivot_at_confirmation(
            highs,
            lows,
            index,
        )

        changed = False

        # Same bar PH + PL -> original Pine excludes it.
        if ph is not None and pl is None and pivot_index is not None:
            changed = self._pivot_update(state, ph, 1, pivot_index)
        elif pl is not None and ph is None and pivot_index is not None:
            changed = self._pivot_update(state, pl, -1, pivot_index)

        self._try_create_pattern(
            state=state,
            symbol=symbol,
            timeframe=timeframe,
            bar_index=bar_index,
            open_time=open_time,
            close=float(row["close"]),
            changed=changed,
            log_setup=False,
        )

        state.last_closed_index = bar_index
        state.last_closed_open_time = open_time

    def _rebuild_from_history(
        self,
        *,
        symbol: str,
        timeframe: str,
        df: pd.DataFrame,
        current_candle: Optional[dict],
    ) -> PairState:
        state = PairState()

        if df is None or df.empty:
            state.initialized = True
            return state

        rows = (
            df
            .sort_values("open_time")
            .reset_index(drop=True)
        )

        highs = [float(v) for v in rows["high"].tolist()]
        lows = [float(v) for v in rows["low"].tolist()]

        for i in range(len(rows)):
            self._process_history_bar(
                state=state,
                symbol=symbol,
                timeframe=timeframe,
                rows=rows,
                highs=highs,
                lows=lows,
                index=i,
            )

        # Current bar touch history is restored but never emitted at startup.
        if current_candle is not None:
            self._touch_targets(
                state=state,
                symbol=symbol,
                timeframe=timeframe,
                bar_index=state.last_closed_index + 1,
                low=float(current_candle["low"]),
                high=float(current_candle["high"]),
                ts_ms=int(current_candle["open_time"]),
                emit=False,
            )

        state.initialized = True
        return state

    # ========================================================
    # CLOSED CANDLE
    # ========================================================

    def on_candle(
        self,
        symbol: str,
        timeframe: str,
        df: pd.DataFrame,
        current_candle: Optional[dict],
        startup: bool,
    ) -> list[Signal]:
        key = self._key(symbol, timeframe)
        lock = self._pair_lock(symbol, timeframe)

        with lock:
            if startup or key not in self._states or not self._states[key].initialized:
                self._states[key] = self._rebuild_from_history(
                    symbol=symbol,
                    timeframe=timeframe,
                    df=df,
                    current_candle=current_candle,
                )
                self._sync_live_interest(
                    symbol,
                    timeframe,
                    self._states[key],
                )
                return []

            state = self._states[key]

            if df is None or df.empty:
                return []

            rows = (
                df
                .sort_values("open_time")
                .reset_index(drop=True)
            )

            row = rows.iloc[-1]
            open_time = int(row["open_time"])

            if (
                state.last_closed_open_time is not None
                and open_time <= state.last_closed_open_time
            ):
                return []

            bar_index = state.last_closed_index + 1

            # 1) Existing target touches on this just-closed bar.
            signals = self._touch_targets(
                state=state,
                symbol=symbol,
                timeframe=timeframe,
                bar_index=bar_index,
                low=float(row["low"]),
                high=float(row["high"]),
                ts_ms=int(row["close_time"]),
                emit=True,
            )

            # 2) Invalidate previous patterns.
            self._invalidate_patterns(
                state,
                bar_index=bar_index,
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
            )

            # 3) Confirm pivot.
            highs = [float(v) for v in rows["high"].tolist()]
            lows = [float(v) for v in rows["low"].tolist()]
            local_confirm_index = len(rows) - 1

            ph, pl, local_pivot_index = self._pivot_at_confirmation(
                highs,
                lows,
                local_confirm_index,
            )

            changed = False
            if local_pivot_index is not None:
                pivot_global_index = bar_index - RIGHT_BARS

                if ph is not None and pl is None:
                    changed = self._pivot_update(
                        state,
                        ph,
                        1,
                        pivot_global_index,
                    )
                elif pl is not None and ph is None:
                    changed = self._pivot_update(
                        state,
                        pl,
                        -1,
                        pivot_global_index,
                    )

            # 4) Create new pattern.
            setup_signals = self._try_create_pattern(
                state=state,
                symbol=symbol,
                timeframe=timeframe,
                bar_index=bar_index,
                open_time=open_time,
                close=float(row["close"]),
                changed=changed,
                log_setup=True,
            )

            # Publish newly-created live interest immediately. This narrows the
            # window in which the next candle has started but its trades are
            # still being filtered before the setup becomes visible.
            self._sync_live_interest(
                symbol,
                timeframe,
                state,
            )

            if setup_signals:
                signals.extend(setup_signals)

            # If the just-closed candle created/updated a setup while the next
            # candle had already started, catch up using the current candle's
            # observed high/low. This prevents an early next-bar touch from
            # being lost while the closed-candle event was waiting to run.
            if current_candle is not None:
                current_open = int(current_candle.get("open_time", 0) or 0)
                if current_open > open_time:
                    step_ms = int(TIMEFRAME_SECONDS[timeframe]) * 1000
                    bars_ahead = max(1, (current_open - open_time) // step_ms)
                    catchup_bar_index = bar_index + int(bars_ahead)

                    catchup_signals = self._touch_targets(
                        state=state,
                        symbol=symbol,
                        timeframe=timeframe,
                        bar_index=catchup_bar_index,
                        low=float(current_candle["low"]),
                        high=float(current_candle["high"]),
                        ts_ms=int(
                            current_candle.get("last_ts_ms")
                            or current_candle.get("open_time")
                            or open_time
                        ),
                        emit=True,
                    )
                    if catchup_signals:
                        signals.extend(catchup_signals)

            state.last_closed_index = bar_index
            state.last_closed_open_time = open_time

            self._sync_live_interest(
                symbol,
                timeframe,
                state,
            )

            return signals

    # ========================================================
    # LIVE TRADE
    # ========================================================

    def on_trade(
        self,
        symbol: str,
        timeframe: str,
        price: float,
        ts_ms: int,
        live_bar: LiveBar,
    ) -> list[Signal]:
        key = self._key(symbol, timeframe)
        lock = self._pair_lock(symbol, timeframe)

        with lock:
            state = self._states.get(key)

            if state is None or not state.initialized:
                return []

            signals = self._touch_targets(
                state=state,
                symbol=symbol,
                timeframe=timeframe,
                bar_index=state.last_closed_index + 1,
                low=float(live_bar.low),
                high=float(live_bar.high),
                ts_ms=int(ts_ms),
                emit=True,
            )

            self._sync_live_interest(
                symbol,
                timeframe,
                state,
            )
            return signals

    # ========================================================
    # PERSISTENT GLOBAL STATE
    # ========================================================
    # The pair pattern state is rebuilt from confirmed candle history.
    # Durable signal dedup is handled by common_engine SQLite outbox.

    def get_state(self) -> dict:
        return {}

    def load_state(self, state: dict) -> None:
        pass


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    configure_runtime(
        # Exactly three Telegram groups.
        symbol_groups=SYMBOL_GROUPS,

        # V2/PRO automatically creates abc_state_okx.sqlite3 beside this.
        state_file=Path("abc_state_okx.json"),
        candle_cache_file=Path("abc_candle_cache.pkl"),

        timeframes=TIMEFRAMES,

        # Auto-discover every current OKX USDT perpetual.
        auto_discover_symbols=True,
        usdt_only=True,

        exclude_symbols=EXCLUDE_SYMBOLS,
    )

    configure_indicators(
        [
            GorillaABCIndicator(),
        ]
    )

    main()
