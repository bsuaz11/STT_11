# STT_11 — Capital Allocation

## 1. How we're splitting $1M across roles

| Seat | Bot ID | Allocation | % of budget |
|---|---|---:|---:|
| 🏛 Exchange license | venue on port 8767 | $300,000 | 30% |
| 🏦 Broker desk 1 | `stt_11_broker_1` | $300,000 | 30% |
| 🏦 Broker desk 2 | `stt_11_broker_2` | $100,000 | 10% |
| 🤖 Trader seat 1 | `stt_11_trader_1` | $100,000 | 10% |
| 🤖 Trader seat 2 | `stt_11_trader_2` | $100,000 | 10% |
| 🤖 Trader seat 3 | `stt_11_trader_3` | $100,000 | 10% |
| **Total** | | **$1,000,000** | **100%** |

By role: exchange $300k (30%), brokers $400k (40%), traders $300k (30%).
Every dollar is allocated; nothing is left unspent.

Key considerations for allocation decision:

- **Fixed prices and limits.** The license costs a flat $300k. A broker desk
  needs at least $100k (max 2 desks) and a trader seat at least $50k (max 5).
- **Free shares don't scale with seats.** The starting grant (20 shares per
  equity) is split across a team's seats, so our 5 bots get 4 shares each per
  symbol. Extra seats add no free inventory.
- **Limits are per bot.** The message quota and the 1,000-share position limit
  apply to each bot separately, so each extra seat adds capacity.
- **More bots = More strategies** Having multiple trader and broker seats allows us to deploy various strategies, diversifying our exposure during trading. 
- **Our venue's fees:** 14 bps taker, 11 bps maker rebate, netting 3 bps on
  every trade (the floor is 2). This undercuts the arena default of 15/10, but may change before exchanges are made live.

## 2. Justification for split

**Exchange license (30%).** It is the only seat that earns money without
having to be right about prices. It collects 3 bps on every trade matched at
our venue (with current fee structure), whichever way the market goes. Our fees undercut the default on
both sides: takers pay less, and makers get a bigger rebate. The aim is to
draw brokers' quotes, and the flow that follows them, to our venue. The cost
is that $300k is spent on the license and never becomes trading cash. We
accept that because fee income doesn't depend on the arena's direction, while
everything else we own does.

**Brokers (40%), one large desk and one minimum desk.** Market making needs
the most capital. A broker has to hold inventory on both sides, and until
week 4 it can only offer shares it already owns. It also pays margin interest
and is liquidated below maintenance. So we put $300k into our main desk: that
gives it room to absorb inventory swings without being forced out at a bad
price. The second desk sits at the $100k minimum. It doubles our message
quota and position capacity for quoting, and lets us run different spread and
skew settings against the main desk on live flow without risking most of the
broker capital.

**Traders (30%), three seats of $100k.** We chose a middle ground between few
large seats and many small ones:

- **Not 1–2 large seats.** A single seat concentrates our trading capital on one
 strategy, and one bug would take down all our trading. Limits are per bot, so 
 one seat also caps our total message and position capacity.
- **Not 5 small seats.** Free shares don't scale with seats: five seats would
  just split the same grant thinner. Each seat also needs its own strategy
  that we can build, monitor and explain, and three is what our team can
  maintain well.
- **Why $100k each.** It is double the minimum and well above what a
  position-limited strategy uses, so a seat can take a full-size position in
  a high-priced stock without running out of cash after a few losing trades.

**Overall.** About 70% of the budget (license plus brokers) goes to the
roles that earn from flow: fees, spread and rebates. These earn steadily
whichever way prices move. The remaining 30% goes to traders, which are the
higher-variance, directional part of the portfolio. If our venue fails to
attract flow, the license is our biggest sunk cost. That is the main risk of
this split.