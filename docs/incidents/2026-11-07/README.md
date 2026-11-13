# Incident 2026-11-07: streaming dropped 21% of a burst replay

**Detected by:** `recon_daily_summary` (1% gap test warned): batch 19,998 events, streaming 15,754, gap 4,244 (21.2%).

**Cause:** the producer sent a full day of event time to Kinesis in seconds. The two shards were read at
different speeds, the watermark (1 minute) jumped ahead with whichever shard led, and Spark dropped the
other shard's records as late, with no error.

| Phase | What happened | Lost |
|---|---|---|
| Start | Shard A read to ~08:30 before shard B started; watermark jumped to ~08:29 | Shard B 00:00–07:30: 2,146 |
| Middle | Shard B raced to ~16:00 while A sat at 08:30; watermark jumped again | Shard A 08:30–16:00: 2,082 |
| End | Last 2-minute window held by the watermark (expected, predicted in advance) | 16 |
| **Total** | | **4,244** |

**Evidence:** `by_postal_code.csv` (loss splits cleanly by shard), `by_shard_hour.csv` (whole hours missing per shard).

**Fix:** paced replay (`producer.py --minutes-per-day 24`, event-time order, no throttling) and a 5-minute
watermark for replays. Backfills belong in batch, not streaming.
