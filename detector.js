/* detector.js — automatic issue detection algorithms
   All detection is simulation-based, not OS-level.
   Implements: deadlock detection (wait-for graph),
   race condition detection, bottleneck analysis,
   improper sync detection, starvation detection.
*/

const Detector = {

  /* ── 1. DEADLOCK DETECTION (Wait-For Graph)
     Builds a dependency graph: P → Q means P is waiting for
     a resource held by Q. A cycle = deadlock.
  ──────────────────────────────────────────── */
  detectDeadlock() {
    const graph = {};  // adjacency list: pid → [pid]
    State.procs.forEach(p => { graph[p.id] = []; });

    // Build wait-for edges from resourceHolding / resourceWaiting
    State.procs.forEach(p => {
      const waiting = State.resourceWaiting[p.id];
      if (waiting !== null && waiting !== undefined) {
        // Find who holds the resource p is waiting for
        State.procs.forEach(q => {
          if (p.id !== q.id && State.resourceHolding[q.id]?.includes(waiting)) {
            graph[p.id].push(q.id);
          }
        });
      }
    });

    // DFS cycle detection
    const visited = {}, recStack = {};
    const hasCycle = (node) => {
      visited[node] = true; recStack[node] = true;
      for (const neighbor of (graph[node] || [])) {
        if (!visited[neighbor] && hasCycle(neighbor)) return true;
        if (recStack[neighbor]) return true;
      }
      recStack[node] = false;
      return false;
    };

    const cycleNodes = [];
    State.procs.forEach(p => {
      if (!visited[p.id] && hasCycle(p.id)) cycleNodes.push(p.id);
    });

    return cycleNodes.length > 0
      ? { detected: true, nodes: cycleNodes, graph }
      : { detected: false };
  },

  /* ── 2. RACE CONDITION DETECTION
     Detects when multiple processes access same resource
     without proper locking (unsynchronized concurrent access).
  ──────────────────────────────────────────── */
  detectRaceCondition() {
    if (State.mode !== 'shm') return { detected: false };

    const corrupted = State.shm.segs.filter(s => s.state === 'corrupted');
    const unlockedWritten = State.shm.segs.filter(s => s.state === 'written');

    // Race: multiple processes running simultaneously + unlocked shared segs
    const runningProcs = State.procs.filter(p => p.state === 'run');
    const mtxW = State.getPrim('mtx_write');
    const mtxR = State.getPrim('mtx_read');
    const mutexFree = mtxW?.state === 'free' && mtxR?.state === 'free';

    if (corrupted.length > 0) {
      return {
        detected: true, severity: 'critical',
        reason: `${corrupted.length} segment(s) corrupted by concurrent unsynchronized access`,
        segs: corrupted.map(s => s.id)
      };
    }
    if (runningProcs.length >= 2 && unlockedWritten.length > 0 && mutexFree) {
      return {
        detected: true, severity: 'warning',
        reason: `${runningProcs.length} processes accessing shared memory without mutex lock`,
        segs: []
      };
    }
    return { detected: false };
  },

  /* ── 3. IMPROPER SYNCHRONIZATION DETECTION
     Detects: missing mutex, semaphore misuse,
     priority inversion in message queues.
  ──────────────────────────────────────────── */
  detectImproperSync() {
    const issues = [];

    // Priority inversion: low-pri message blocking high-pri
    if (State.mode === 'msgq' && State.msgq.q.length >= 2) {
      const head = State.msgq.q[0];
      const tail = State.msgq.q[State.msgq.q.length - 1];
      if (tail && head && tail.pri < head.pri) {
        issues.push({
          type: 'priority_inversion',
          msg: `priority inversion — msg pri=${tail.pri} enqueued after pri=${head.pri} head`
        });
      }
    }

    // Semaphore underflow: consumer running with sem_full=0
    const semFull = State.getPrim('sem_full');
    if (semFull && semFull.count <= 0 && State.getR()?.state === 'run') {
      issues.push({
        type: 'semaphore_underflow',
        msg: 'sem_full=0 but consumer still reading — semaphore not enforced'
      });
    }

    // Mutex double-lock detection: same proc holds both mutexes
    State.procs.forEach(p => {
      const held = State.resourceHolding[p.id] || [];
      if (held.includes('mtx_write') && held.includes('mtx_read')) {
        issues.push({ type: 'double_lock', msg: `${p.name} holds mtx_write + mtx_read simultaneously — potential deadlock` });
      }
    });

    return issues;
  },

  /* ── 4. BOTTLENECK ANALYSIS
     Analyzes producer/consumer rates and identifies
     where the throughput bottleneck is.
  ──────────────────────────────────────────── */
  analyzeBottleneck() {
    const T = State.throughput;
    if (State.tick < 5) return null;

    const prodRate = T.windowSent.length / T.WINDOW;
    const consRate = T.windowRcvd.length / T.WINDOW;
    const ratio = consRate > 0 ? (prodRate / consRate).toFixed(2) : '∞';

    let diagnosis = null;
    if (prodRate > consRate * 1.5) {
      diagnosis = {
        type: 'producer_faster',
        severity: 'warn',
        msg: `producer ${(prodRate*100).toFixed(0)}% faster than consumer — buffer will overflow`,
        reason: 'Consumer cannot keep up. Increase consumer threads or reduce write rate.',
        ratio
      };
    } else if (consRate > prodRate * 1.5) {
      diagnosis = {
        type: 'consumer_faster',
        severity: 'info',
        msg: `consumer starving — producer rate too low (ratio ${ratio})`,
        reason: 'Consumer idles waiting for data. Increase producer rate or add more writers.',
        ratio
      };
    }

    // Buffer occupancy bottleneck
    if (State.mode === 'pipe') {
      const fill = State.pipe.buf.length / State.pipe.cap;
      if (fill > 0.85) {
        diagnosis = {
          type: 'buffer_overflow_risk',
          severity: 'err',
          msg: `pipe buffer ${Math.round(fill*100)}% full — overflow imminent`,
          reason: 'Producer enqueues faster than consumer drains. Writer will block on next write.',
          ratio
        };
      }
    }

    return { prodRate: (prodRate*100).toFixed(0)+'%', consRate: (consRate*100).toFixed(0)+'%', ratio, diagnosis };
  },

  /* ── 5. STARVATION DETECTION
     A process that has been waiting >N ticks continuously
     is considered starved.
  ──────────────────────────────────────────── */
  detectStarvation() {
    const THRESHOLD = 8;
    const starved = State.procs.filter(p =>
      (State.waitHistory[p.id] || 0) >= THRESHOLD
    );
    return starved.length > 0
      ? { detected: true, procs: starved.map(p => p.name), ticks: THRESHOLD }
      : { detected: false };
  },

  /* ── 6. MUTEX / SEMAPHORE STATE CHECK ── */
  checkPrimitives() {
    const issues = [];
    const allBlocked = State.procs.every(p => p.state === 'block' || p.state === 'wait');
    State.primitives.forEach(prim => {
      if (prim.state === 'deadlock') {
        issues.push({ type: 'prim_deadlock', msg: `${prim.label} in deadlock state` });
      }
      if (prim.type === 'semaphore' && prim.count < 0) {
        issues.push({ type: 'sem_negative', msg: `${prim.label} count=${prim.count} — semaphore underflow` });
      }
    });
    return issues;
  },

  /* ── MASTER ANALYSIS — runs every tick in auto mode ── */
  runAll() {
    const results = {
      deadlock:    this.detectDeadlock(),
      race:        this.detectRaceCondition(),
      sync:        this.detectImproperSync(),
      bottleneck:  this.analyzeBottleneck(),
      starvation:  this.detectStarvation(),
      primitives:  this.checkPrimitives(),
    };
    return results;
  }
};
