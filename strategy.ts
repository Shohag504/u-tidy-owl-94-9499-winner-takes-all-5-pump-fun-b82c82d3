// Rider: an early-breakout rider for the pump.fun bonding-curve replay.
//
// Idea (from the dev window's own shape): coins pump in their first ~50 slots after creation
// and then fade back toward their create price, so holding is what loses money. The edge, if any,
// is in catching a confirmed early run-up, banking part of it quickly, and letting the rest ride
// behind a trailing stop — never holding a faded coin. Entry needs a real crowd (several distinct
// buyers, positive recent net flow) and a price already up but not already spent. Exits are
// mechanical: a hard stop, a partial take-profit, a trailing stop once up, a hold cap, and an
// instant exit when the creator sells or a big seller shows up.
//
// Guardrail pacing: the run must end with round trips on at least 20 coins, so when fills or
// coins lag the pace the bot opens tiny scout round trips on coins that pass a weaker version
// of the main entry filter, exiting on a take-profit, a small stop, a trailing stop, or a
// hold cap — so the guardrail fills are real trades instead of pure fee burn. Pacing is
// capped at about 25 coins: the marginal scout past the strongest early signals was a net
// loser on the dev window, so the bot stops scouting once the bar is comfortably met.
//
// Pure function of the batches it is fed: no clock, no randomness, no I/O. Iteration only over
// insertion-ordered maps fed by the batch stream, so the two graded executions agree.

import type { Batch, Bot, BotState, OrderInput, Pubkey } from './vendor/makefun-bot.ts';

export interface Params {
  /** Entry age window (slots since CreateEvent). */
  minAge: number;
  maxAge: number;
  /** Price now over create price at entry: window. */
  minRunup: number;
  maxRunup: number;
  /** Distinct buyers seen on the coin before entry. */
  minBuyers: number;
  /** Net SOL (buys minus sells) over the flow window at entry. */
  minFlowSol: number;
  /** Flow window in slots. */
  flowWindow: number;
  /** Price now at least this share of the recent high. */
  nearHigh: number;
  /** Recent-high window in slots. */
  highWindow: number;
  /** Real SOL reserve window at entry. */
  minRqSol: number;
  maxRqSol: number;
  /** Biggest single sell (SOL) tolerated in the flow window at entry. */
  maxSellSol: number;
  /** First buy (SOL) and most SOL per position. */
  firstSol: number;
  maxSol: number;
  /** One add-on buy (SOL), once, when the price stepped up. */
  addSol: number;
  addStep: number;
  /** Smallest buy worth sending. */
  minBuySol: number;
  /** Slippage on buys (bps); sells go out wide open. */
  buySlipBps: number;
  /** Hard stop: exit when value falls to cost * (1 - stopLoss). */
  stopLoss: number;
  /** Partial take-profit: sell half when value reaches cost * (1 + tpPart). */
  tpPart: number;
  /** Full take-profit: sell all when value reaches cost * (1 + tpFull). */
  tpFull: number;
  /** Trailing stop arms once value reaches cost * (1 + trailArm); exits on a fall of trail from peak. */
  trailArm: number;
  trail: number;
  /** Exit after this many slots held (unless riding: value up at least rideUp). */
  maxHold: number;
  rideUp: number;
  /** Exit when net flow over the flow window falls below -exitFlowSol. */
  exitFlowSol: number;
  /** Exit on any single recorded sell of at least this many SOL. */
  bigSellSol: number;
  /** Slots to wait after an exit before the same coin may be bought again. */
  cooldown: number;
  /** Most entries per coin over the run. */
  maxEntries: number;
  /** Most main positions open at once. */
  maxMain: number;
  /** Most SOL at cost across all positions at once. */
  maxExposureSol: number;
  /** Guardrail pacing targets. */
  targetCoins: number;
  targetFills: number;
  paceSecs: number;
  probeSol: number;
  maxProbes: number;
  /** Scout (guardrail) entries: a weaker filter than the main entry. */
  scoutMinBuyers: number;
  scoutMaxAge: number;
  scoutMinRunup: number;
  scoutMaxRunup: number;
  scoutTp: number;
  scoutSl: number;
  scoutMaxHold: number;
  /** Trailing stop for scouts: once the scout's peak value is up by scoutTrailArm, exit when
   *  it falls scoutTrail below the peak, instead of giving the whole spike back. */
  scoutTrailArm: number;
  scoutTrail: number;
}

export const DEFAULT_PARAMS: Params = {
  minAge: 2,
  maxAge: 10,
  minRunup: 1.0,
  maxRunup: 3.0,
  minBuyers: 20,
  minFlowSol: 0.2,
  flowWindow: 15,
  nearHigh: 0.95,
  highWindow: 20,
  minRqSol: 0.3,
  maxRqSol: 100,
  maxSellSol: 0.5,
  firstSol: 0.3,
  maxSol: 0.3,
  addSol: 0.1,
  addStep: 0.15,
  minBuySol: 0.01,
  buySlipBps: 1500,
  stopLoss: 0.05,
  tpPart: 0.4,
  tpFull: 0.3,
  trailArm: 0.25,
  trail: 0.1,
  maxHold: 150,
  rideUp: 0.25,
  exitFlowSol: 0.5,
  bigSellSol: 1.0,
  cooldown: 100,
  maxEntries: 2,
  maxMain: 5,
  maxExposureSol: 4,
  targetCoins: 25,
  targetFills: 40,
  paceSecs: 14400,
  probeSol: 0.01,
  maxProbes: 2,
  scoutMinBuyers: 6,
  scoutMaxAge: 30,
  scoutMinRunup: 1.0,
  scoutMaxRunup: 2.5,
  scoutTp: 0.25,
  scoutSl: 0.15,
  scoutMaxHold: 60,
  scoutTrailArm: 0.15,
  scoutTrail: 0.15,
};

const LAMPORTS = 1e9;
const CURVE_FEE = 0.0125;

interface Trade {
  slot: number;
  buy: boolean;
  sol: number;
  user: string;
  price: number;
}

interface Tape {
  created: number | null;
  creator: string | null;
  p0: number;
  trades: Trade[];
  entries: number;
  lastExit: number;
  complete: boolean;
}

interface Held {
  opened: number;
  peakValue: number;
  invested: number;
  exiting: boolean;
  filled: boolean;
  partialTaken: boolean;
  adds: number;
  lastBuyPrice: number;
  lastValue: number;
}

export function createStrategy(p: Params = DEFAULT_PARAMS): Bot {
  const tapes = new Map<Pubkey, Tape>();
  const held = new Map<Pubkey, Held>();
  /** Scout round trips: mint -> {opened slot, peak value}. A scout is a 0.01 SOL probe that
   *  exits on a small take-profit, a stop, or a hold cap — not an instant sell — so guardrail
   *  fills pay for themselves instead of bleeding fees. */
  const probes = new Map<Pubkey, { opened: number; peak: number }>();
  const scouted = new Set<Pubkey>();
  let myFills = 0;
  const myCoins = new Set<Pubkey>();
  let firstTime: number | null = null;
  let lastTime: number | null = null;
  const keep = Math.max(p.flowWindow, p.highWindow, p.maxAge) + 10;

  const tapeOf = (mint: Pubkey): Tape => {
    let t = tapes.get(mint);
    if (t === undefined) {
      t = { created: null, creator: null, p0: 0, trades: [], entries: 0, lastExit: -1e12, complete: false };
      tapes.set(mint, t);
    }
    return t;
  };

  /** Net SOL flow and buyer stats over the last `w` slots, plus the window high. */
  const windowStats = (t: Tape, now: number, w: number) => {
    let net = 0;
    let high = 0;
    let bigSell = 0;
    let devSold = false;
    const buyers = new Set<string>();
    for (let i = t.trades.length - 1; i >= 0; i--) {
      const tr = t.trades[i]!;
      if (tr.slot <= now - w) break;
      net += tr.buy ? tr.sol : -tr.sol;
      if (tr.price > high) high = tr.price;
      if (tr.buy) buyers.add(tr.user);
      else {
        if (tr.sol > bigSell) bigSell = tr.sol;
        if (tr.user === t.creator) devSold = true;
      }
    }
    return { net, high, bigSell, devSold, buyers: buyers.size };
  };

  const entryOk = (t: Tape, now: number, price: number, rqSol: number): boolean => {
    if (t.created === null || t.p0 <= 0) return false;
    const age = now - t.created;
    if (age < p.minAge || age > p.maxAge) return false;
    if (rqSol < p.minRqSol || rqSol > p.maxRqSol) return false;
    const runup = price / t.p0;
    if (runup < p.minRunup || runup > p.maxRunup) return false;
    const w = windowStats(t, now, p.flowWindow);
    if (w.net < p.minFlowSol) return false;
    if (w.bigSell >= p.maxSellSol) return false;
    if (w.devSold) return false;
    if (w.buyers < p.minBuyers) return false;
    const hw = windowStats(t, now, p.highWindow);
    if (price < hw.high * p.nearHigh) return false;
    return true;
  };

  return {
    onBatch(state: BotState, batch: Batch): OrderInput[] {
      const now = batch.slot;
      const devSoldNow = new Set<Pubkey>();
      const bigSellNow = new Set<Pubkey>();
      const traded = new Set<Pubkey>();
      for (const f of batch.fills) {
        if (f.status === 'filled' && !f.client_id.startsWith('_')) {
          myFills++;
          myCoins.add(f.mint);
        }
      }
      for (const e of batch.events) {
        if (e.block_time !== null) {
          if (firstTime === null) firstTime = e.block_time;
          if (lastTime === null || e.block_time > lastTime) lastTime = e.block_time;
        }
        if (e.event === 'CreateEvent') {
          const t = tapeOf(e.mint);
          t.created = e.slot;
          t.creator = e.creator;
          t.p0 = Number(e.virtual_quote_reserves) / Number(e.virtual_token_reserves);
        } else if (e.event === 'TradeEvent') {
          const t = tapeOf(e.mint);
          const sol = Number(e.sol_amount) / LAMPORTS;
          t.trades.push({ slot: e.slot, buy: e.is_buy, sol, user: e.user, price: Number(e.virtual_quote_reserves) / Number(e.virtual_token_reserves) });
          traded.add(e.mint);
          if (!e.is_buy && e.user === t.creator) devSoldNow.add(e.mint);
          if (!e.is_buy && sol >= p.bigSellSol) bigSellNow.add(e.mint);
        } else if (e.event === 'CompleteEvent') {
          tapeOf(e.mint).complete = true;
        }
      }
      for (const mint of traded) {
        const t = tapes.get(mint)!;
        let cut = 0;
        while (cut < t.trades.length && t.trades[cut]!.slot <= now - keep) cut++;
        if (cut > 0) t.trades.splice(0, cut);
      }
      const marks = new Map(batch.marks.map((m) => [m.mint, m] as const));
      const pendingByMint = new Map<Pubkey, number>();
      for (const po of state.pending.values()) pendingByMint.set(po.order.mint, (pendingByMint.get(po.order.mint) ?? 0) + 1);

      const orders: OrderInput[] = [];
      const cap = state.hello.caps.max_orders_per_batch;
      const tx = Number(state.txFeeLamports()) / LAMPORTS;

      const valueOf = (mint: Pubkey): { value: number; cost: number; tokens: number } => {
        const pos = state.positions.get(mint)!;
        const curve = state.curves.get(mint);
        const mk = marks.get(mint);
        const vq = mk ? Number(mk.virtual_quote_reserves) : Number(curve!.virtual_quote_reserves);
        const vt = mk ? Number(mk.virtual_token_reserves) : Number(curve!.virtual_token_reserves);
        const tokens = Number(pos.token_amount);
        const value = ((tokens * vq) / (vt + tokens) / LAMPORTS) * (1 - CURVE_FEE) - tx;
        return { value, cost: Number(pos.cost_lamports) / LAMPORTS, tokens };
      };

      // Exits first: stops, take-profits, trailing stop, hold cap, creator/big-sell/flow exits.
      for (const [mint, h] of held) {
        const pos = state.positions.get(mint);
        const curve = state.curves.get(mint);
        if (pos === undefined) {
          if (!pendingByMint.has(mint)) {
            held.delete(mint);
            const t = tapeOf(mint);
            if (h.filled) t.lastExit = now;
            else t.entries--;
          }
          continue;
        }
        h.filled = true;
        if (curve === undefined || curve.complete) continue;
        const { value, cost } = valueOf(mint);
        if (value > h.peakValue) h.peakValue = value;
        h.lastValue = value;
        const t = tapeOf(mint);
        const riding = value >= cost * (1 + p.rideUp);
        let exit = false;
        let partial = false;
        if (value < cost * (1 - p.stopLoss)) exit = true;
        else if (!h.partialTaken && value >= cost * (1 + p.tpPart)) partial = true;
        else if (value >= cost * (1 + p.tpFull)) exit = true;
        else if (h.peakValue >= cost * (1 + p.trailArm) && value < h.peakValue * (1 - p.trail)) exit = true;
        else if (now - h.opened > p.maxHold && !riding) exit = true;
        else if (devSoldNow.has(mint) || bigSellNow.has(mint)) exit = true;
        else if (windowStats(t, now, p.flowWindow).net < -p.exitFlowSol) exit = true;
        if (!exit && !partial) continue;
        let sellPending = false;
        for (const po of state.pending.values()) if (po.order.mint === mint && po.order.side === 'sell') sellPending = true;
        if (sellPending) continue;
        if (orders.length >= cap) break;
        if (partial) {
          const half = BigInt(Math.floor(Number(pos.token_amount) / 2));
          if (half <= 0n) continue;
          orders.push({ side: 'sell', mint, token_amount: half, max_slippage_bps: 10_000 });
          h.partialTaken = true;
        } else {
          orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
          h.exiting = true;
        }
      }

      // Scout exits: take a small profit, cut a small loss, or time out — never hold a faded coin.
      for (const [mint, sc] of probes) {
        const pos = state.positions.get(mint);
        const curve = state.curves.get(mint);
        if (pos === undefined) {
          if (!pendingByMint.has(mint)) probes.delete(mint);
          continue;
        }
        if (curve === undefined || curve.complete || pendingByMint.has(mint)) continue;
        const mk = marks.get(mint);
        const vq = mk ? Number(mk.virtual_quote_reserves) : Number(curve.virtual_quote_reserves);
        const vt = mk ? Number(mk.virtual_token_reserves) : Number(curve.virtual_token_reserves);
        const tokens = Number(pos.token_amount);
        const value = ((tokens * vq) / (vt + tokens) / LAMPORTS) * (1 - CURVE_FEE) - tx;
        const cost = Number(pos.cost_lamports) / LAMPORTS;
        if (value > sc.peak) sc.peak = value;
        const done =
          value >= cost * (1 + p.scoutTp) ||
          value <= cost * (1 - p.scoutSl) ||
          now - sc.opened > p.scoutMaxHold ||
          (sc.peak >= cost * (1 + p.scoutTrailArm) && value < sc.peak * (1 - p.scoutTrail)) ||
          devSoldNow.has(mint) ||
          bigSellNow.has(mint);
        if (!done) continue;
        if (orders.length >= cap) break;
        orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
      }

      // Entries and one add-on per position.
      let free = Number(state.freeCashLamports()) / LAMPORTS;
      let slotsLeft = state.hello.caps.max_open_positions - state.committedPositionCount();
      let mainLeft = p.maxMain - held.size;
      let exposure = 0;
      for (const [mint, h] of held) exposure += Math.max(h.invested, Number(state.positions.get(mint)?.cost_lamports ?? 0n) / LAMPORTS);
      for (const mint of traded) {
        if (orders.length >= cap) break;
        const curve = state.curves.get(mint);
        if (curve === undefined || curve.complete) continue;
        if (pendingByMint.has(mint) || probes.has(mint)) continue;
        const t = tapes.get(mint)!;
        const h = held.get(mint);
        const price = Number(curve.virtual_quote_reserves) / Number(curve.virtual_token_reserves);
        const rqSol = Number(curve.real_quote_reserves) / LAMPORTS;
        let want: number;
        if (h === undefined) {
          if (slotsLeft <= 0 || mainLeft <= 0) continue;
          if (t.entries >= p.maxEntries || now - t.lastExit < p.cooldown) continue;
          if (!entryOk(t, now, price, rqSol)) continue;
          want = p.firstSol;
        } else {
          if (h.exiting || !h.filled || h.adds >= 1 || h.invested >= p.maxSol - p.minBuySol) continue;
          const pos = state.positions.get(mint);
          if (pos === undefined) continue;
          if (price < h.lastBuyPrice * (1 + p.addStep)) continue;
          if (windowStats(t, now, p.flowWindow).net < p.minFlowSol) continue;
          if (rqSol > p.maxRqSol) continue;
          want = Math.min(p.addSol, p.maxSol - h.invested);
        }
        const capSol = Math.floor(Number(state.maxBuyLamports(mint))) / LAMPORTS;
        let amount = Math.min(capSol, want, p.maxExposureSol - exposure);
        if (amount > free - tx) amount = free - tx;
        if (amount < p.minBuySol) continue;
        const lamports = BigInt(Math.floor(amount * LAMPORTS));
        orders.push({ side: 'buy', mint, quote_lamports: lamports, max_slippage_bps: p.buySlipBps });
        free -= amount + tx;
        exposure += amount;
        if (h === undefined) {
          held.set(mint, { opened: now, peakValue: 0, invested: amount, exiting: false, filled: false, partialTaken: false, adds: 0, lastBuyPrice: price, lastValue: 0 });
          t.entries++;
          slotsLeft--;
          mainLeft--;
        } else {
          h.invested += amount;
          h.adds++;
          h.lastBuyPrice = price;
        }
      }

      // Guardrail pacing: open a scout when fills or coins lag the pace. A scout must pass a
      // weaker version of the main entry filter, so its fills are real trades, not fee burn.
      if (probes.size < p.maxProbes && slotsLeft > 0 && firstTime !== null && lastTime !== null) {
        const pace = Math.min(1, (lastTime - firstTime) / p.paceSecs);
        const needCoins = myCoins.size < Math.ceil(p.targetCoins * pace);
        const needFills = myFills < Math.ceil(p.targetFills * pace);
        if (needCoins || needFills) {
          const minRq = (p.probeSol * 10_000) / state.hello.caps.max_order_bps_of_real_reserve;
          for (const mint of traded) {
            if (orders.length >= cap || probes.size >= p.maxProbes) break;
            if (held.has(mint) || probes.has(mint) || scouted.has(mint) || pendingByMint.has(mint) || state.positions.has(mint)) continue;
            if (needCoins && !needFills && myCoins.has(mint)) continue;
            const curve = state.curves.get(mint);
            if (curve === undefined || curve.complete) continue;
            if (Number(curve.real_quote_reserves) / LAMPORTS < minRq * 1.5) continue;
            const t = tapes.get(mint)!;
            if (t.created === null || now - t.created > p.scoutMaxAge) continue;
            const price = Number(curve.virtual_quote_reserves) / Number(curve.virtual_token_reserves);
            if (t.p0 <= 0) continue;
            const runup = price / t.p0;
            if (runup < p.scoutMinRunup || runup > p.scoutMaxRunup) continue;
            const w = windowStats(t, now, p.flowWindow);
            if (w.buyers < p.scoutMinBuyers || w.devSold || w.net < 0) continue;
            if (free < p.probeSol + 2 * tx) break;
            orders.push({ side: 'buy', mint, quote_lamports: BigInt(Math.floor(p.probeSol * LAMPORTS)), max_slippage_bps: p.buySlipBps });
            probes.set(mint, { opened: now, peak: 0 });
            scouted.add(mint);
            free -= p.probeSol + tx;
          }
        }
      }
      return orders;
    },
  };
}
