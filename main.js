/* main.js — bootstrap, landing screen, app control (v4) */

/* ── LANDING SCREEN ── */
function launchMode(mode) {
  State.appMode = mode;
  document.getElementById('landing').classList.add('hidden');
  document.getElementById('app').classList.remove('hidden');

  const badge = document.getElementById('mode-badge');
  badge.textContent = mode;
  badge.className = mode === 'auto' ? 'auto-badge' : 'manual-badge';

  // Show/hide manual-only controls
  document.querySelectorAll('.manual-only').forEach(el => {
    el.classList.toggle('hidden', mode !== 'manual');
  });
  document.getElementById('manual-controls')?.classList.toggle('hidden', mode !== 'manual');
  document.getElementById('semaphore-ctl')?.classList.toggle('hidden', mode !== 'manual');

  State.initProcs();
  State.initPrimitives();
  renderAll();
  renderSyncPrimitives();
  addLog('info', `${mode === 'auto' ? 'AUTOMATIC' : 'MANUAL'} mode — press run to start simulation`);
  if (mode === 'auto') {
    addLog('info', 'auto-detection active: deadlock (wait-for graph), race conditions, bottleneck, starvation');
  } else {
    addLog('info', 'manual mode: use inject buttons to trigger faults and observe system response');
  }
}

function goHome() {
  clearTimeout(State.timer);
  State.running = false;
  document.getElementById('app').classList.add('hidden');
  document.getElementById('landing').classList.remove('hidden');
  const btn = document.getElementById('toggle-btn');
  if (btn) { btn.classList.remove('paused'); btn.innerHTML = '<span class="btn-dot"></span>run'; }
}

/* ── SIMULATION LOOP ── */
function toggleSim() {
  State.running = !State.running;
  const btn = document.getElementById('toggle-btn');
  if (State.running) {
    btn.classList.remove('paused');
    btn.innerHTML = '<span class="btn-dot"></span>pause';
    schedNext();
  } else {
    btn.classList.add('paused');
    btn.innerHTML = '<span class="btn-dot"></span>run';
    clearTimeout(State.timer);
  }
}
function schedNext() {
  if (!State.running) return;
  State.tick++;
  simulateStep();
  State.timer = setTimeout(schedNext, State.speed);
}

/* ── CONTROLS ── */
function setSpeed(v) {
  State.speed = Number(v);
  document.getElementById('speed-lbl').textContent = (State.speed / 1000).toFixed(1) + 's';
}

function setBufSize(v) {
  State.pipe.cap = Number(v);
  State.pipe.buf = State.pipe.buf.slice(0, State.pipe.cap);
  addLog('info', `pipe buffer size set to ${v}`);
  renderAll();
}

function setSemaphore(v) {
  State.semaphoreMax = Number(v);
  const sem = State.getPrim('sem_empty');
  if (sem) { sem.max = State.semaphoreMax; sem.count = State.semaphoreMax; sem.label = `sem(${v})`; }
  addLog('info', `semaphore count set to ${v}`);
  renderSyncPrimitives();
}

function switchTab(t) {
  ['pipe','msgq','shm'].forEach(k => {
    const panel = document.getElementById('tp-' + k);
    if (panel) panel.classList.toggle('hidden', k !== t);
    const btn = document.querySelector(`.tab[data-tab="${k}"]`);
    if (btn) btn.classList.toggle('active', k === t);
  });
}

function changeMode(v) {
  State.mode = v;
  resetAll();
  switchTab(v === 'pipe' ? 'pipe' : v === 'msgq' ? 'msgq' : 'shm');
}

function rebuildProcs(n) {
  State.numProcs = Number(n);
  resetAll();
}

function resetAll() {
  State.reset();
  clearLog();
  const btn = document.getElementById('toggle-btn');
  if (btn) { btn.classList.remove('paused'); btn.innerHTML = '<span class="btn-dot"></span>run'; }
  const bnBtn = document.getElementById('bn-btn');
  if (bnBtn) { bnBtn.textContent = 'inject bottleneck'; bnBtn.classList.remove('active-fault'); }
  const raceBtn = document.getElementById('race-btn');
  if (raceBtn) { raceBtn.textContent = 'inject race'; raceBtn.classList.remove('active-fault'); }
  renderAll();
  renderSyncPrimitives();
  addLog('info', 'reset — system re-initialised');
}

console.info('%c IPC Debugger v4 ', 'background:#5ba4f5;color:#fff;font-weight:bold;padding:3px 8px;border-radius:4px');
console.info('Open with Live Server for hot-reload. Two modes: auto (algorithmic detection) + manual (fault injection).');
