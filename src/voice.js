import { eligibleVoice } from './domain.js';

export class VoiceTracker {
  constructor({ service, guildId, onError = () => {}, onGap = () => {} }) {
    Object.assign(this, { service, guildId, onError, onGap });
    this.snapshot = []; this.rule = null; this.last = null;
    this.epoch = 0; this.tail = Promise.resolve();
    // Only intervals captured while eligibility was known are queued. Per-user
    // ordering plus the stored voiceUntil watermark make retries idempotent.
    this.pending = new Map();
  }
  enqueue(action) {
    const epoch = this.epoch;
    const operation = this.tail.then(() => epoch === this.epoch ? action() : undefined);
    this.tail = operation.catch(error => {
      if (epoch === this.epoch && !error.voiceRetryable) this.drop();
      this.onError(error);
    });
    return operation;
  }
  transition(snapshot, rule, memberIds, at) {
    const eligible = structuredClone(eligibleVoice(snapshot, rule, memberIds));
    // Capture the old interval and the new state synchronously, before waiting
    // for MongoDB. A later disconnect must not erase already received events.
    if (this.capture(at)) {
      this.snapshot = eligible; this.rule = structuredClone(rule);
    }
    return this.enqueue(() => this.persist());
  }
  tick(at) {
    this.capture(at);
    return this.enqueue(() => this.persist());
  }
  capture(at) {
    if (!Number.isFinite(at)) throw new TypeError('Invalid voice observation time');
    // Old callbacks cannot rewind the cursor or restore an older voice state.
    if (this.last !== null && at < this.last) return false;
    const from = this.last; this.last = at;
    // This limit applies to observation gaps, never to persistence delays.
    if (from !== null && at > from && at - from <= 60000 && this.rule) {
      for (const member of this.snapshot) {
        if (!this.pending.has(member.userId)) this.pending.set(member.userId, []);
        this.pending.get(member.userId).push({ event: {
          guildId: this.guildId, userId: member.userId, channelId: member.channelId,
          ...(member.categoryId ? { categoryId: member.categoryId } : {}),
          from, to: at, eligible: true
        }, rule: structuredClone(this.rule) });
      }
    } else if (from !== null && at - from > 60000 && this.snapshot.length) {
      this.onGap({ from, to: at, memberCount: this.snapshot.length });
    }
    return true;
  }
  async persist() {
    const epoch = this.epoch, jobs = [...this.pending], failures = [];
    let cursor = 0;
    // Bounded parallelism avoids one slow member delaying every other member.
    const worker = async () => {
      while (cursor < jobs.length && epoch === this.epoch) {
        const [id, queue] = jobs[cursor++];
        while (queue.length && epoch === this.epoch) {
          const item = queue[0];
          try { await this.service.voice(item.event, item.rule); queue.shift(); }
          catch (error) { failures.push(error); break; }
        }
        if (!queue.length && this.pending.get(id) === queue) this.pending.delete(id);
      }
    };
    await Promise.all(Array.from({ length: Math.min(8, jobs.length) }, worker));
    if (failures.length) throw Object.assign(new Error(`تعذر حفظ وقت الصوت لـ ${failures.length} عضو؛ ستعاد محاولة الفترات المرصودة.`, { cause: failures[0] }), { voiceRetryable: true });
  }
  drop() {
    this.epoch++; this.snapshot = []; this.last = null; this.rule = null;
    // Keep previously observed intervals; never add disconnected time.
  }
  async drain() { await this.tail; }
}
