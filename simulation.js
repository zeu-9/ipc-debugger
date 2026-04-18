/* simulation.js — IPC simulation engine (v4)
   Manages sync primitives, resource tracking,
   auto-triggered faults in auto mode.
*/

function simulateStep() {
  if (State.dlActive) {
    // Manual deadlock
    State.procs.forEach(p => p.state = 'block');
    State.primitives.forEach(p => { p.state = 'deadlock'; });
    addLog('err', 'DEADLOCK active — all processes blocked (circular wait)');
  } else {
    if (State.mode === 'pipe') simPipe();
    else if (State.mode === 'msgq') simMsgQ();
    else simSHM();
  }

  // Auto mode: trigger organic issues probabilistically
  if (State.appMode === 'auto') autoOrganicFaults();

  State.pushTL();
  updateSyncPrimitives();
  renderAll();
}

/* ── AUTO ORGANIC FAULTS (only in auto mode) ── */
function autoOrganicFaults() {
  // Gradually speed up producer to create bottleneck naturally
  if (State.tick > 15 && State.tick % 20 === 0 && State.mode === 'pipe') {
    const w = State.getW();
    if (w && State.pipe.buf.length > State.pipe.cap * 0.5) {
      addLog('warn', '[AUTO] producer rate increasing — buffer pressure rising');
    }
  }
  // Auto-inject race condition in SHM after tick 10
  if (State.appMode === 'auto' && State.mode === 'shm' && State.tick > 10 && Math.random() < 0.06) {
    const written = State.shm.segs.filter(s => s.state === 'written');
    if (written.length > 0) {
      const s = written[Math.floor(Math.random() * written.length)];
      s.state = 'corrupted';
      addLog('err', `[AUTO] race condition detected — seg[${s.id}] written without mutex lock`);
    }
  }
  // Auto resource contention for deadlock simulation
  if (State.appMode === 'auto' && State.tick > 8 && State.tick % 25 === 0) {
    const procs = State.procs;
    if (procs.length >= 2) {
      const r1 = 'res_A', r2 = 'res_B';
      State.resourceHolding[0] = [r1];
      State.resourceWaiting[0] = r2;
      State.resourceHolding[1] = [r2];
      State.resourceWaiting[1] = r1;
      addLog('warn', `[AUTO] resource conflict — P0 holds ${r1} wants ${r2}; P1 holds ${r2} wants ${r1}`);
      // Clear after a few ticks
      setTimeout(() => {
        if (State.resourceHolding[0]) State.resourceHolding[0] = [];
        if (State.resourceWaiting[0] !== undefined) State.resourceWaiting[0] = null;
        if (State.resourceHolding[1]) State.resourceHolding[1] = [];
        if (State.resourceWaiting[1] !== undefined) State.resourceWaiting[1] = null;
      }, State.speed * 4);
    }
  }
}

/* ── PIPE ── */
function simPipe() {
  const w = State.getW(), r = State.getR(), rel = State.getRel();
  const writeProb = State.bnActive ? 0.92 : (State.appMode === 'auto' && State.tick > 20 ? 0.72 : 0.60);
  const readProb  = State.bnActive ? 0.14 : 0.64;

  // Acquire write mutex
  const mtxW = State.getPrim('mtx_write');
  if (Math.random() < writeProb) {
    if (pipe_acquireMutex(mtxW, w)) {
      if (State.pipe.buf.length < State.pipe.cap) {
        State.pipe.buf.push({ id: State.tick, col: w.col, bg: w.bg });
        w.state = 'run'; State.stats.sent++;
        State.throughput.windowSent.push(State.tick);
        addLog('ok', `${w.name} wrote pkt#${State.tick} [mutex_W acquired]`);
      } else {
        w.state = 'wait'; State.stats.drop++;
        State.dropHistory.push(State.tick);
        addLog('warn', `${w.name} blocked — pipe buffer full (${State.pipe.cap})`);
      }
      pipe_releaseMutex(mtxW);
    } else {
      w.state = 'wait';
      addLog('warn', `${w.name} waiting for mutex_W (held by ${mtxW.owner})`);
    }
  } else { w.state = 'idle'; }

  rel.forEach(rp => {
    if (Math.random() < 0.38 && State.pipe.buf.length > 0) {
      const pk = State.pipe.buf.splice(0, 1)[0];
      pk.col = rp.col; pk.bg = rp.bg;
      State.pipe.buf.push(pk); rp.state = 'run';
      addLog('info', `${rp.name} relayed pkt#${pk.id}`);
    } else rp.state = Math.random() < 0.3 ? 'wait' : 'idle';
  });

  // Acquire read mutex
  const mtxR = State.getPrim('mtx_read');
  if (State.pipe.buf.length > 0 && Math.random() < readProb) {
    if (pipe_acquireMutex(mtxR, r)) {
      const pk = State.pipe.buf.splice(0, 1)[0];
      r.state = 'run'; State.stats.rcvd++;
      State.throughput.windowRcvd.push(State.tick);
      const lat = Math.round((State.tick - pk.id) * State.speed / 10);
      State.stats.lats.push(lat);
      addLog('ok', `${r.name} read pkt#${pk.id} (~${lat}ms) [mutex_R acquired]`);
      pipe_releaseMutex(mtxR);
    } else {
      r.state = 'wait';
      addLog('warn', `${r.name} waiting for mutex_R (held by ${mtxR.owner})`);
    }
  } else r.state = State.pipe.buf.length ? 'wait' : 'idle';

  // Update semaphores
  const semEmpty = State.getPrim('sem_empty');
  const semFull  = State.getPrim('sem_full');
  if (semEmpty) semEmpty.count = Math.max(0, State.pipe.cap - State.pipe.buf.length);
  if (semFull)  semFull.count  = State.pipe.buf.length;

  renderPipe();
}

function pipe_acquireMutex(mtx, proc) {
  if (!mtx) return true;
  if (mtx.state === 'free') {
    mtx.state = 'acquired';
    mtx.owner = proc?.name || '?';
    return true;
  }
  mtx.state = 'waiting';
  return false;
}
function pipe_releaseMutex(mtx) {
  if (!mtx) return;
  mtx.state = 'free';
  mtx.owner = null;
}

/* ── MSG QUEUE ── */
function simMsgQ() {
  const w = State.getW(), r = State.getR(), rel = State.getRel();
  if (Math.random() < 0.55) {
    if (State.msgq.q.length < State.msgq.cap) {
      const pri = Math.ceil(Math.random() * 3);
      State.msgq.q.push({ id: State.tick, pri, from: w.name, data: `msg_${State.tick}` });
      State.msgq.q.sort((a, b) => a.pri - b.pri);
      State.msgq.sent++; State.stats.sent++; w.state = 'run';
      // Update semaphore count
      const semFull = State.getPrim('sem_full');
      if (semFull) semFull.count = Math.min(semFull.max, State.msgq.q.length);
      addLog('ok', `${w.name} enqueued msg_${State.tick} pri=${pri}`);
    } else {
      w.state = 'wait'; State.stats.drop++;
      State.dropHistory.push(State.tick);
      addLog('warn', `${w.name} blocked — queue full (${State.msgq.cap})`);
    }
  } else w.state = 'idle';

  rel.forEach(rp => { rp.state = Math.random() < 0.28 ? 'run' : 'idle'; });

  if (State.msgq.q.length > 0 && Math.random() < 0.50) {
    const m = State.msgq.q.shift(); State.msgq.rcvd++; State.stats.rcvd++; r.state = 'run';
    State.throughput.windowRcvd.push(State.tick);
    const semFull = State.getPrim('sem_full');
    if (semFull) semFull.count = Math.max(0, State.msgq.q.length);
    addLog('ok', `${r.name} dequeued ${m.data} (pri=${m.pri})`);
  } else r.state = State.msgq.q.length ? 'wait' : 'idle';
  renderMsgQ();
}

/* ── SHARED MEMORY ── */
function simSHM() {
  const w = State.getW(), r = State.getR(), rel = State.getRel();
  const free    = State.shm.segs.filter(s => s.state === 'free');
  const written = State.shm.segs.filter(s => s.state === 'written');
  const mtxW = State.getPrim('mtx_write');
  const mtxR = State.getPrim('mtx_read');

  // Writer with mutex
  if (free.length > 0 && Math.random() < 0.50) {
    if (pipe_acquireMutex(mtxW, w)) {
      const s = free[Math.floor(Math.random() * free.length)];
      s.state = 'written'; s.owner = w.name;
      s.val = '0x' + (State.tick * 7).toString(16).padStart(4, '0');
      w.state = 'run'; State.stats.sent++;
      State.resourceHolding[w.id] = ['shm_seg_' + s.id];
      addLog('ok', `${w.name} wrote seg[${s.id}]=${s.val} [mutex_W]`);
      pipe_releaseMutex(mtxW);
      State.resourceHolding[w.id] = [];
    } else {
      w.state = 'wait';
    }
  } else w.state = 'idle';

  // Bottleneck: lock contention
  if (State.bnActive && Math.random() < 0.38) {
    const s = State.shm.segs[Math.floor(Math.random() * State.shm.segs.length)];
    if (s.state !== 'free') {
      s.state = 'locked'; s.owner = 'mutex';
      mtxW.state = 'acquired'; mtxW.owner = 'contention';
      addLog('warn', `seg[${s.id}] mutex contention — improper lock management`);
    }
  }
  // Release locked segs
  State.shm.segs.filter(s => s.state === 'locked').forEach(s => {
    if (Math.random() < 0.38) { s.state = 'written'; s.owner = w?.name; }
  });

  // Relay race conditions (unsynchronized access)
  rel.forEach(rp => {
    const raceChance = State.raceActive ? 0.22 : (State.appMode === 'auto' ? 0.06 : 0.04);
    if (Math.random() < raceChance && written.length > 0) {
      const s = written[Math.floor(Math.random() * written.length)];
      s.state = 'corrupted'; rp.state = 'run';
      State.resourceHolding[rp.id] = ['shm_seg_' + s.id];
      addLog('err', `${rp.name} RACE CONDITION — seg[${s.id}] written without mutex (unsynchronized)`);
      State.resourceHolding[rp.id] = [];
    } else rp.state = Math.random() < 0.22 ? 'run' : 'idle';
  });

  // Auto-repair corrupted
  State.shm.segs.filter(s => s.state === 'corrupted').forEach(s => {
    if (Math.random() < 0.18) { s.state = 'free'; s.owner = null; s.val = null; }
  });

  // Reader with mutex
  const rd = State.shm.segs.filter(s => s.state === 'written');
  if (rd.length > 0 && Math.random() < 0.54) {
    if (pipe_acquireMutex(mtxR, r)) {
      const s = rd[Math.floor(Math.random() * rd.length)]; const v = s.val;
      s.state = 'free'; s.owner = null; s.val = null;
      r.state = 'run'; State.stats.rcvd++;
      State.throughput.windowRcvd.push(State.tick);
      addLog('ok', `${r.name} read seg[${s.id}]=${v} [mutex_R]`);
      pipe_releaseMutex(mtxR);
    } else r.state = 'wait';
  } else r.state = rd.length ? 'wait' : 'idle';

  // Update semaphores
  const semFull = State.getPrim('sem_full');
  const semEmpty = State.getPrim('sem_empty');
  if (semFull)  semFull.count  = State.shm.segs.filter(s => s.state === 'written').length;
  if (semEmpty) semEmpty.count = Math.max(0, 32 - semFull?.count || 32);

  renderSHM();
}

/* ── SYNC PRIMITIVE RENDERING UPDATE ── */
function updateSyncPrimitives() {
  // Occasionally simulate mutex release race
  if (State.appMode === 'auto' && Math.random() < 0.1) {
    State.primitives.filter(p => p.state === 'waiting').forEach(p => {
      p.state = 'free'; p.owner = null;
    });
  }
  renderSyncPrimitives();
}

/* ── MANUAL FAULT INJECTORS ── */
function injectDeadlock() {
  State.dlActive = true;
  // Set up circular wait manually
  if (State.procs.length >= 2) {
    State.resourceHolding[0] = ['res_A'];
    State.resourceWaiting[0] = 'res_B';
    State.resourceHolding[1] = ['res_B'];
    State.resourceWaiting[1] = 'res_A';
  }
  State.procs.forEach(p => p.state = 'block');
  State.primitives.forEach(p => { p.state = 'deadlock'; p.owner = 'deadlock'; });
  addLog('err', 'INJECTED deadlock — P0 holds res_A waits res_B; P1 holds res_B waits res_A');
  renderAll();
}

function toggleBottleneck() {
  State.bnActive = !State.bnActive;
  const btn = document.getElementById('bn-btn');
  btn?.classList.toggle('active-fault', State.bnActive);
  if (btn) btn.textContent = State.bnActive ? 'clear bottleneck' : 'inject bottleneck';
  addLog(State.bnActive ? 'warn' : 'info',
    State.bnActive ? 'INJECTED bottleneck — high producer rate, low consumer rate, mutex contention' : 'bottleneck cleared');
}

function toggleRaceCondition() {
  State.raceActive = !State.raceActive;
  const btn = document.getElementById('race-btn');
  btn?.classList.toggle('active-fault', State.raceActive);
  if (btn) btn.textContent = State.raceActive ? 'clear race' : 'inject race';
  addLog(State.raceActive ? 'err' : 'info',
    State.raceActive ? 'INJECTED race condition — relay processes bypass mutex locks' : 'race condition cleared');
}
