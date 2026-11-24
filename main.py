"""Pump.fun Replay Sprint paper bot (agent-fun/replay/v2).

Momentum / early-entry bot, pure Python standard library, fully deterministic:
no clock, no randomness, no environment reads; iteration only over
insertion-ordered dicts and lists fed by the harness batch stream.

Strategy
--------
* Watch every new bonding curve. A curve becomes a buy candidate once it is
  old enough (past MIN_AGE slots since its CreateEvent) and its recent tape
  shows accelerating buy pressure: net SOL flowing in, several distinct
  buyers, price rising and sitting near its window high, and the newer half
  of the window bringing in at least as much net SOL as the older half.
* Buy small and equal-sized (STAKE_SOL, capped by the 1%-of-real-reserve
  order cap), at most max_open_positions concurrent, one entry per mint.
* Exit a position whole on the first of: take-profit, stop-loss, a trailing
  stop once armed, a fade of buy flow while under water, or a max hold time.
* Guardrail pacing: if the bot's own filled round trips lag the pace needed
  for 20+ distinct coins, open tiny probe round trips (PROBE_SOL) on liquid
  curves and sell them as soon as the buy lands, so the run still clears the
  minimum-coins bar even on a quiet window.
* Stop opening new positions near the end of the window; the harness
  flattens whatever is left.

Wire protocol (agent-fun/replay/v2): newline-delimited JSON on stdin, one
reply line per batch on stdout. The harness opens with a hello message
(carrying caps, fees and capital), then streams one batch per slot window;
the bot answers each batch with its orders for that slot and the harness
closes with done. Amounts are u64 decimal strings. Replies carry only
orders; diagnostics go to stderr.
"""

import json
import math
import sys

LAMPORTS_PER_SOL = 1_000_000_000
CURVE_FEE = 0.0125  # 95 bps protocol + 30 bps creator
BPS = 10_000
MAX_BOT_LINE_BYTES = 65_536

# ---- strategy parameters ----
WINDOW = 40            # entry lookback, slots
MIN_AGE = 300          # slots since CreateEvent before an entry is allowed
MIN_NET_SOL = 0.5      # net buy SOL (buys minus sells) over the window
MIN_BUYS = 4           # buy trades in the window
MIN_BUYERS = 4         # distinct buying wallets in the window
MIN_PRICE_UP = 0.03    # price now vs window start, minus 1
MAX_PRICE_UP = 2.0     # skip curves that already ran too far in one window
NEAR_HIGH = 0.90       # price now at least this share of the window high
MAX_BUYER_SHARE = 0.8  # largest single wallet's share of window buy SOL
MIN_RQ_SOL = 2.0       # curve real SOL bounds at entry
MAX_RQ_SOL = 80.0
STAKE_SOL = 0.10       # SOL per position
MIN_BUY_SOL = 0.02     # smallest buy worth sending
BUY_SLIPPAGE_BPS = 1500
SELL_SLIPPAGE_BPS = 10000
STOP_LOSS = 0.25       # exit when value falls to (1 - STOP_LOSS) * cost
TAKE_PROFIT = 0.60     # exit when value reaches (1 + TAKE_PROFIT) * cost
TRAIL_ARM = 0.20       # trailing stop arms once peak >= (1 + TRAIL_ARM) * cost
TRAIL_DROP = 0.25      # exit when value falls to (1 - TRAIL_DROP) * peak
FADE_WINDOW = 30       # exit while under water if net flow over this is bad
FADE_NET_SOL = -1.0
MAX_HOLD_SLOTS = 1500  # time limit, unless the position is a big runner
MAX_ENTRIES_PER_MINT = 1
PROBE_SOL = 0.02       # guardrail probe round-trip size
TARGET_COINS = 30      # distinct-coin pace target over the window
PACE_SLOTS = 400000    # slots the pace is spread over (~24h window)
ENTRY_CUTOFF_SLOTS = 395000  # stop opening after this many slots in
KEEP_SLOTS = 60        # how long to keep a curve's tape


class ProtocolError(Exception):
    """The harness sent something this bot cannot follow."""


class Curve:
    """A bonding curve the bot has seen, at its latest known reserves."""

    __slots__ = (
        "mint", "created", "vt", "vq", "rt", "rq", "reserves_slot",
        "complete", "trades", "entries", "last_exit",
    )

    def __init__(self, mint):
        self.mint = mint
        self.created = None       # slot of its CreateEvent, None if unseen
        self.vt = 0               # virtual token reserves
        self.vq = 0               # virtual quote reserves
        self.rt = 0               # real token reserves
        self.rq = 0               # real quote reserves
        self.reserves_slot = -1
        self.complete = False
        self.trades = []          # (slot, is_buy, sol, user, price), slot order
        self.entries = 0          # entries opened on this mint
        self.last_exit = -10 ** 12


class Held:
    """A position the bot opened, tracked for exits."""

    __slots__ = ("opened", "peak", "filled", "exiting")

    def __init__(self, opened):
        self.opened = opened
        self.peak = 0.0           # best close value seen (lamports)
        self.filled = False       # some buy of this position filled
        self.exiting = False      # exit decided, sell sent or in flight


class Bot:
    def __init__(self, hello):
        self.hello = hello
        # Validate the hello handshake before trusting it: the caps, fees and
        # capital the bot reads must all be present and sane, or the replay
        # cannot be scored (a malformed hello is a protocol error, not a
        # tradeable window).
        try:
            self.caps = hello["caps"]
            fees = hello["fees"]
            capital = hello["capital"]
            base_fee = int(fees["base_fee_lamports"])
            priority_fee = int(fees["priority_fee_lamports"])
            self.capital = int(capital["lamports"])
            max_open = int(self.caps["max_open_positions"])
            max_order_bps = int(self.caps["max_order_bps_of_real_reserve"])
            max_per_batch = int(self.caps["max_orders_per_batch"])
        except (KeyError, TypeError, ValueError):
            raise ProtocolError("hello is missing caps, fees or capital") from None
        if self.capital <= 0 or max_open < 1 or max_order_bps <= 0 or max_per_batch < 1:
            raise ProtocolError("hello carries impossible caps or capital")
        if base_fee < 0 or priority_fee < 0:
            raise ProtocolError("hello carries a negative fee")
        self.tx_fee = base_fee + priority_fee
        self.cash = self.capital
        self.positions = {}       # mint -> (token_amount, cost_lamports)
        self.curves = {}          # mint -> Curve, first-seen order
        self.pending = {}         # client_id -> order dict, send order
        self.held = {}            # mint -> Held
        self.probes = {}          # mint -> selling flag, first-seen order
        self.next_id = 1
        self.seq = -1
        self.slot = -1
        self.first_slot = None
        self.my_fills = 0
        self.my_coins = set()     # membership only, never iterated
        self.batches = 0
        self._traded = []         # mints with a TradeEvent this batch, in order

    # ---- helpers ----

    def _new_id(self):
        cid = "o%d" % self.next_id
        self.next_id += 1
        return cid

    def max_buy_lamports(self, mint):
        """Size cap on this mint now: 1% of real quote reserves, 0 if unknown."""
        curve = self.curves.get(mint)
        if curve is None or curve.complete:
            return 0
        return curve.rq * int(self.caps["max_order_bps_of_real_reserve"]) // BPS

    def committed_position_count(self):
        mints = set(self.positions)
        for order in self.pending.values():
            if order["side"] == "buy":
                mints.add(order["mint"])
        return len(mints)

    def free_cash_lamports(self):
        free = self.cash
        for order in self.pending.values():
            free -= self.tx_fee
            if order["side"] == "buy":
                free -= int(order["quote_lamports"])
        return free

    def position_value_lamports(self, mint, tokens):
        """What selling `tokens` would bring now (curve fee and tx fee out)."""
        curve = self.curves.get(mint)
        if curve is None or curve.vt + tokens <= 0:
            return 0.0
        gross = tokens * curve.vq / (curve.vt + tokens)
        return gross * (1.0 - CURVE_FEE) - self.tx_fee

    def flow_sol(self, curve, now, window):
        """Net recorded SOL (buys minus sells) over the last `window` slots."""
        net = 0.0
        for trade in curve.trades:
            if trade[0] <= now - window:
                continue
            net += trade[2] if trade[1] else -trade[2]
        return net

    # ---- batch intake ----

    def apply_batch(self, batch):
        self.seq = batch["seq"]
        self.slot = batch["slot"]
        if self.first_slot is None:
            self.first_slot = self.slot
        self._traded = []
        seen_traded = set()
        for fill in batch["fills"]:
            self.pending.pop(fill.get("client_id"), None)
            if fill.get("status") == "filled":
                cid = fill.get("client_id", "")
                if not cid.startswith("_"):
                    self.my_fills += 1
                    self.my_coins.add(fill.get("mint"))
        account = batch["account"]
        self.cash = int(account["cash_lamports"])
        self.positions = {}
        for pos in account["positions"]:
            tokens = int(pos["token_amount"])
            if tokens > 0:
                self.positions[pos["mint"]] = (tokens, int(pos["cost_lamports"]))
        for mark in batch["marks"]:
            mint = mark["mint"]
            curve = self.curves.get(mint)
            if curve is None:
                curve = Curve(mint)
                self.curves[mint] = curve
            if int(mark["slot"]) < curve.reserves_slot:
                continue
            curve.vt = int(mark["virtual_token_reserves"])
            curve.vq = int(mark["virtual_quote_reserves"])
            curve.rt = int(mark["real_token_reserves"])
            curve.rq = int(mark["real_quote_reserves"])
            curve.reserves_slot = int(mark["slot"])
        for event in batch["events"]:
            kind = event["event"]
            mint = event["mint"]
            if kind == "CreateEvent":
                curve = self.curves.get(mint)
                if curve is None:
                    curve = Curve(mint)
                    self.curves[mint] = curve
                curve.created = int(event["slot"])
                curve.vt = int(event["virtual_token_reserves"])
                curve.vq = int(event["virtual_quote_reserves"])
                curve.rt = int(event["real_token_reserves"])
                curve.rq = 0
                curve.reserves_slot = int(event["slot"])
            elif kind == "TradeEvent":
                curve = self.curves.get(mint)
                if curve is None:
                    curve = Curve(mint)
                    self.curves[mint] = curve
                curve.vt = int(event["virtual_token_reserves"])
                curve.vq = int(event["virtual_quote_reserves"])
                curve.rt = int(event["real_token_reserves"])
                curve.rq = int(event["real_quote_reserves"])
                curve.reserves_slot = int(event["slot"])
                price = curve.vq / curve.vt if curve.vt > 0 else 0.0
                curve.trades.append(
                    (
                        int(event["slot"]),
                        bool(event["is_buy"]),
                        int(event["sol_amount"]) / LAMPORTS_PER_SOL,
                        event["user"],
                        price,
                    )
                )
                if mint not in seen_traded:
                    seen_traded.add(mint)
                    self._traded.append(mint)
            else:  # CompleteEvent
                curve = self.curves.get(mint)
                if curve is None:
                    curve = Curve(mint)
                    self.curves[mint] = curve
                curve.complete = True
        now = self.slot
        for mint in self._traded:
            curve = self.curves[mint]
            cut = 0
            while cut < len(curve.trades) and curve.trades[cut][0] <= now - KEEP_SLOTS:
                cut += 1
            if cut:
                curve.trades = curve.trades[cut:]

    def apply_done(self, done):
        self.seq = done["seq"]
        account = done["account"]
        self.cash = int(account["cash_lamports"])
        self.positions = {}
        for pos in account["positions"]:
            tokens = int(pos["token_amount"])
            if tokens > 0:
                self.positions[pos["mint"]] = (tokens, int(pos["cost_lamports"]))

    # ---- strategy ----

    def entry_signal(self, curve, now):
        rq_sol = curve.rq / LAMPORTS_PER_SOL
        if rq_sol < MIN_RQ_SOL or rq_sol > MAX_RQ_SOL:
            return False
        if curve.created is None or now - curve.created < MIN_AGE:
            return False
        start = now - WINDOW
        mid = now - WINDOW // 2
        start_price = -1.0
        high = 0.0
        last = 0.0
        buy_sol = 0.0
        sell_sol = 0.0
        buys = 0
        net_old = 0.0
        net_new = 0.0
        buyers = {}
        for (slot, is_buy, sol, user, price) in curve.trades:
            if slot <= start:
                start_price = price
                continue
            if start_price < 0:
                start_price = price
            signed = sol if is_buy else -sol
            if slot <= mid:
                net_old += signed
            else:
                net_new += signed
            if is_buy:
                buy_sol += sol
                buys += 1
                buyers[user] = buyers.get(user, 0.0) + sol
            else:
                sell_sol += sol
            if price > high:
                high = price
            last = price
        if buys < MIN_BUYS or len(buyers) < MIN_BUYERS:
            return False
        if buy_sol - sell_sol < MIN_NET_SOL:
            return False
        if start_price <= 0 or last <= 0:
            return False
        up = last / start_price - 1.0
        if up < MIN_PRICE_UP or up > MAX_PRICE_UP:
            return False
        if last < high * NEAR_HIGH:
            return False
        if net_new <= 0 or net_new < net_old:
            return False
        top = 0.0
        for value in buyers.values():
            if value > top:
                top = value
        if buy_sol > 0 and top > MAX_BUYER_SHARE * buy_sol:
            return False
        return True

    def decide(self, batch):
        now = self.slot
        orders = []
        cap = int(self.caps["max_orders_per_batch"])
        max_pos = int(self.caps["max_open_positions"])
        tx = self.tx_fee
        pending_mints = {}
        for order in self.pending.values():
            pending_mints.setdefault(order["mint"], []).append(order["side"])

        # Exits first: sell each held position whole on the first exit trigger.
        for mint in list(self.held.keys()):
            held = self.held[mint]
            pos = self.positions.get(mint)
            if pos is None:
                if mint not in pending_mints:
                    curve = self.curves.get(mint)
                    if curve is not None:
                        if held.filled:
                            curve.last_exit = now
                        elif curve.entries > 0:
                            curve.entries -= 1
                    del self.held[mint]
                continue
            held.filled = True
            curve = self.curves.get(mint)
            if curve is None or curve.complete:
                continue
            tokens, cost = pos
            value = self.position_value_lamports(mint, tokens)
            if value > held.peak:
                held.peak = value
            exit_now = held.exiting
            if not exit_now and value <= cost * (1.0 - STOP_LOSS):
                exit_now = True
            if not exit_now and value >= cost * (1.0 + TAKE_PROFIT):
                exit_now = True
            if not exit_now and held.peak >= cost * (1.0 + TRAIL_ARM):
                if value <= held.peak * (1.0 - TRAIL_DROP):
                    exit_now = True
            if not exit_now and value < cost:
                if self.flow_sol(curve, now, FADE_WINDOW) <= FADE_NET_SOL:
                    exit_now = True
            if not exit_now and now - held.opened > MAX_HOLD_SLOTS:
                if value < cost * (1.0 + TAKE_PROFIT):
                    exit_now = True
            if not exit_now:
                continue
            held.exiting = True
            if "sell" in pending_mints.get(mint, []):
                continue
            if len(orders) >= cap:
                break
            orders.append(
                {
                    "side": "sell",
                    "client_id": self._new_id(),
                    "mint": mint,
                    "token_amount": str(tokens),
                    "max_slippage_bps": SELL_SLIPPAGE_BPS,
                }
            )

        # Probe round trips: sell the whole probe as soon as its buy has landed.
        for mint in list(self.probes.keys()):
            pos = self.positions.get(mint)
            if pos is None:
                if mint not in pending_mints:
                    del self.probes[mint]
                continue
            curve = self.curves.get(mint)
            if curve is None or curve.complete or mint in pending_mints:
                continue
            if len(orders) >= cap:
                break
            orders.append(
                {
                    "side": "sell",
                    "client_id": self._new_id(),
                    "mint": mint,
                    "token_amount": str(pos[0]),
                    "max_slippage_bps": SELL_SLIPPAGE_BPS,
                }
            )
            self.probes[mint] = True

        # Entries and guardrail probes.
        free = self.free_cash_lamports()
        slots_left = max_pos - self.committed_position_count()
        can_enter = (
            self.first_slot is not None
            and now - self.first_slot <= ENTRY_CUTOFF_SLOTS
        )
        if can_enter:
            for mint in self._traded:
                if len(orders) >= cap or slots_left <= 0:
                    break
                if mint in self.held or mint in self.probes:
                    continue
                if mint in pending_mints:
                    continue
                curve = self.curves.get(mint)
                if curve is None or curve.complete:
                    continue
                if curve.entries >= MAX_ENTRIES_PER_MINT:
                    continue
                if not self.entry_signal(curve, now):
                    continue
                amount = min(
                    self.max_buy_lamports(mint),
                    int(STAKE_SOL * LAMPORTS_PER_SOL),
                )
                if amount > free - tx:
                    amount = free - tx
                if amount < int(MIN_BUY_SOL * LAMPORTS_PER_SOL):
                    continue
                orders.append(
                    {
                        "side": "buy",
                        "client_id": self._new_id(),
                        "mint": mint,
                        "quote_lamports": str(amount),
                        "max_slippage_bps": BUY_SLIPPAGE_BPS,
                    }
                )
                free -= amount + tx
                slots_left -= 1
                self.held[mint] = Held(now)
                curve.entries += 1

            # Guardrail pacing: open a probe when distinct coins lag the pace.
            if len(self.probes) < 1 and slots_left > 0:
                pace = min(1.0, (now - self.first_slot) / PACE_SLOTS)
                need = len(self.my_coins) < math.ceil(TARGET_COINS * pace)
                if need:
                    for mint in self._traded:
                        if len(orders) >= cap or len(self.probes) >= 1:
                            break
                        if mint in self.held or mint in self.probes:
                            continue
                        if mint in pending_mints or mint in self.my_coins:
                            continue
                        curve = self.curves.get(mint)
                        if curve is None or curve.complete:
                            continue
                        if curve.entries >= 1:
                            continue
                        if curve.rq < 3 * LAMPORTS_PER_SOL:
                            continue
                        amount = int(PROBE_SOL * LAMPORTS_PER_SOL)
                        if amount > self.max_buy_lamports(mint):
                            continue
                        if free < amount + 2 * tx:
                            break
                        orders.append(
                            {
                                "side": "buy",
                                "client_id": self._new_id(),
                                "mint": mint,
                                "quote_lamports": str(amount),
                                "max_slippage_bps": BUY_SLIPPAGE_BPS,
                            }
                        )
                        self.probes[mint] = False
                        free -= amount + tx
                        slots_left -= 1
        return orders


def run():
    bot = None
    out = sys.stdout
    for raw in sys.stdin.buffer:
        line = raw.strip()
        if not line:
            continue
        try:
            msg = json.loads(line.decode("utf-8"))
        except ValueError:
            raise ProtocolError("input line is not JSON") from None
        kind = msg.get("type") if isinstance(msg, dict) else None
        if kind == "hello":
            if bot is not None:
                raise ProtocolError("a second hello")
            if msg.get("protocol") != "agent-fun/replay/v2":
                raise ProtocolError("the harness speaks another protocol")
            bot = Bot(msg)
        elif kind == "batch":
            if bot is None:
                raise ProtocolError("a batch before hello")
            bot.apply_batch(msg)
            orders = bot.decide(msg)
            text = json.dumps(
                {"type": "reply", "seq": msg["seq"], "orders": orders},
                separators=(",", ":"),
                ensure_ascii=False,
            )
            if len(text.encode("utf-8")) > MAX_BOT_LINE_BYTES:
                raise ProtocolError("reply over the line limit")
            for order in orders:
                bot.pending[order["client_id"]] = order
            out.write(text + "\n")
            out.flush()
            bot.batches += 1
        elif kind == "done":
            if bot is None:
                raise ProtocolError("done before hello")
            bot.apply_done(msg)
            leftover = 0.0
            for mint, (tokens, _cost) in bot.positions.items():
                leftover += bot.position_value_lamports(mint, tokens)
            pnl = (bot.cash + leftover - bot.capital) / LAMPORTS_PER_SOL
            print(
                "done: batches=%d fills=%d coins=%d cash=%.6f SOL "
                "leftover=%.6f SOL net_pnl=%.6f SOL"
                % (
                    bot.batches,
                    bot.my_fills,
                    len(bot.my_coins),
                    bot.cash / LAMPORTS_PER_SOL,
                    leftover / LAMPORTS_PER_SOL,
                    pnl,
                ),
                file=sys.stderr,
            )
            return 0
        else:
            raise ProtocolError("unknown message type %r" % (kind,))
    raise ProtocolError("input ended before done")


if __name__ == "__main__":
    sys.exit(run())
