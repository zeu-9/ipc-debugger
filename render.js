/* render.js — all DOM rendering (v4) */
let logCount = 0;

/* ── PROCESSES ── */
function renderProcs() {
  const c = document.getElementById('proc-container');
  c.innerHTML = '';
  State.procs.forEach(p => {
    const d = document.createElement('div');
    const sc = p.state === 'block' ? ' blocked' : p.state === 'wait' ? ' waiting' : '';
    d.className = 'proc-card' + sc;
    d.id = 'pc-' + p.id;
    d.onclick = () => {
      document.querySelectorAll('.proc-card').forEach(x => x.classList.remove('selected'));
      d.classList.add('selected');
      const held = State.resourceHolding[p.id]?.join(', ') || '—';
      const waiting = State.resourceWaiting[p.id] || '—';
      addLog('info', `inspect ${p.name} pid=${p.pid} state=${p.state} holds=[${held}] waits=${waiting}`);
    };
    const bClass = { run:'pb-run', wait:'pb-wait', block:'pb-block', idle:'pb-idle' }[p.state] || 'pb-idle';
    const bLabel = { run:'running', wait:'waiting', block:'blocked', idle:'idle' }[p.state] || 'idle';
    d.innerHTML = `<div class="proc-name">${p.name}</div>
      <div class="proc-role">${p.role}</div>
      <div class="proc-pid">pid_${p.pid}</div>
      <span class="proc-badge ${bClass}">${bLabel}</span>`;
    c.appendChild(d);
  });
}

/* ── SYNC PRIMITIVES ── */
function renderSyncPrimitives() {
  const c = document.getElementById('sync-primitives');
  if (!c) return;
  c.innerHTML = '';
  State.primitives.forEach(prim => {
    const el = document.createElement('div');
    const stateClass = { free:'sp-free', acquired:'sp-acquired', waiting:'sp-waiting', deadlock:'sp-deadlock' }[prim.state] || 'sp-free';
    el.className = 'sync-prim ' + stateClass;
    const countStr = prim.type === 'semaphore' ? ` [${prim.count}/${prim.max}]` : '';
    const ownerStr = prim.owner ? ` ← ${prim.owner}` : '';
    el.innerHTML = `<span class="sp-indicator"></span>${prim.label}${countStr}${ownerStr}`;
    el.title = `${prim.type}: ${prim.state}${ownerStr}`;
    c.appendChild(el);
  });
}

/* ── PIPE ── */
function renderPipe() {
  const body = document.getElementById('pipe-body');
  if (!body) return;
  body.innerHTML = '';
  State.pipe.buf.slice(0, 22).forEach(pk => {
    const el = document.createElement('div');
    el.className = 'pkt';
    el.style.background = pk.bg;
    el.style.color = pk.col;
    el.style.border = `1px solid ${pk.col}44`;
    el.textContent = '#' + pk.id;
    body.appendChild(el);
  });
  const pct = Math.round(State.pipe.buf.length / State.pipe.cap * 100);
  const bf = document.getElementById('buf-fill');
  const bp = document.getElementById('buf-pct');
  if (bf) bf.style.width = pct + '%';
  if (bp) bp.textContent = pct + '%';
  const w = State.getW(), r = State.getR();
  const pw = document.getElementById('pipe-wr');
  const pr = document.getElementById('pipe-rd');
  if (pw && w) pw.textContent = 'writer: ' + w.name;
  if (pr && r) pr.textContent = 'reader: ' + r.name;
  renderThroughput();
}

function renderThroughput() {
  const bt = Detector.analyzeBottleneck();
  if (!bt) return;
  const el = id => document.getElementById(id);
  if (el('tp-prod')) el('tp-prod').textContent = bt.prodRate;
  if (el('tp-cons')) el('tp-cons').textContent = bt.consRate;
  if (el('tp-ratio')) {
    el('tp-ratio').textContent = bt.ratio;
    el('tp-ratio').style.color = parseFloat(bt.ratio) > 1.5 ? 'var(--red)' : parseFloat(bt.ratio) < 0.67 ? 'var(--amber)' : 'var(--green)';
  }
  const totalRcvd = State.stats.rcvd;
  if (el('tp-thru')) el('tp-thru').textContent = totalRcvd > 0 ? Math.round(totalRcvd / Math.max(1, State.tick) * 100) + '%' : '—';
}

/* ── MSG QUEUE ── */
function renderMsgQ() {
  const col = document.getElementById('queue-col');
  if (!col) return;
  col.innerHTML = '';
  State.msgq.q.slice(-8).forEach(m => {
    const el = document.createElement('div');
    el.className = 'mq-item';
    el.innerHTML = `<b>${m.data}</b> &nbsp;pri=${m.pri} &nbsp;from=${m.from}`;
    col.appendChild(el);
  });
  const d = document.getElementById('mq-depth');
  const s = document.getElementById('mq-sent');
  const rv = document.getElementById('mq-rcvd');
  const p = document.getElementById('mq-pri');
  if (d) d.textContent  = State.msgq.q.length;
  if (s) s.textContent  = State.msgq.sent;
  if (rv) rv.textContent = State.msgq.rcvd;
  if (p) p.textContent  = State.msgq.q.length ? State.msgq.q[0].pri : '—';
}

/* ── SHM ── */
function renderSHM() {
  const g = document.getElementById('shm-grid');
  if (!g) return;
  g.innerHTML = '';
  State.shm.segs.forEach(s => {
    const el = document.createElement('div');
    el.className = 'shm-cell shm-' + s.state;
    el.textContent = s.id;
    el.title = `seg[${s.id}] ${s.state}`;
    el.onclick = () => {
      const det = document.getElementById('shm-detail');
      if (det) det.textContent = `seg[${s.id}]  state:${s.state}  owner:${s.owner||'—'}  val:${s.val||'null'}`;
    };
    g.appendChild(el);
  });
}

/* ── SMART DIAGNOSTICS ── */
function renderIssues() {
  const issues = [];
  const analysis = Detector.runAll();

  // Deadlock
  if (State.dlActive || analysis.deadlock.detected) {
    const nodes = analysis.deadlock.nodes || State.procs.map(p=>p.id);
    issues.push({
      c: 'issue-err',
      h: 'deadlock_detected [wait-for graph cycle]',
      b: `processes ${nodes.map(n=>'P'+n).join(' → ')} form circular dependency`,
      r: 'Detected via wait-for graph DFS traversal. Use resource ordering or Banker\'s algorithm.'
    });
  } else if (State.procs.some(p => p.state === 'block')) {
    const bl = State.procs.filter(p => p.state === 'block');
    issues.push({
      c: 'issue-err',
      h: `${bl.length} process(es) blocked`,
      b: bl.map(p=>p.name).join(', ') + ' — waiting for locked resource',
      r: 'Check mutex hold order and release logic.'
    });
  }

  // Race condition
  if (analysis.race.detected) {
    const sev = analysis.race.severity === 'critical' ? 'issue-err' : 'issue-warn';
    issues.push({
      c: sev,
      h: 'race_condition_detected',
      b: analysis.race.reason,
      r: 'Acquire mutex_W/mutex_R before every shared memory access. Use atomic operations.'
    });
  }

  // Bottleneck
  if (analysis.bottleneck?.diagnosis) {
    const d = analysis.bottleneck.diagnosis;
    const sev = d.severity === 'err' ? 'issue-err' : 'issue-warn';
    issues.push({ c: sev, h: d.msg, b: d.reason, r: `P/C ratio: ${d.ratio}` });
  }

  // Buffer near full
  if (State.mode === 'pipe' && State.pipe.buf.length > State.pipe.cap * 0.8 && !analysis.bottleneck?.diagnosis) {
    issues.push({
      c: 'issue-warn',
      h: 'pipe_buffer_near_full',
      b: `${State.pipe.buf.length}/${State.pipe.cap} slots occupied — writer will block soon`,
      r: 'Increase PIPE_BUF or reduce write frequency.'
    });
  }

  // Queue overflow
  if (State.mode === 'msgq' && State.msgq.q.length >= State.msgq.cap) {
    issues.push({
      c: 'issue-err',
      h: 'message_queue_overflow',
      b: 'queue at max capacity — messages are being dropped',
      r: 'Increase queue capacity or speed up consumer.'
    });
  }

  // Improper sync
  analysis.sync.forEach(s => {
    issues.push({ c: 'issue-warn', h: s.type, b: s.msg, r: 'Review synchronization logic.' });
  });

  // Starvation
  if (analysis.starvation.detected) {
    issues.push({
      c: 'issue-warn',
      h: 'process_starvation',
      b: `${analysis.starvation.procs.join(', ')} waiting >${analysis.starvation.ticks} consecutive ticks`,
      r: 'Use fair scheduling or priority aging to prevent starvation.'
    });
  }

  // Primitive issues
  analysis.primitives.forEach(p => {
    issues.push({ c: 'issue-err', h: p.type, b: p.msg, r: '' });
  });

  // All good
  if (!issues.length) {
    issues.push({
      c: 'issue-ok',
      h: 'system_healthy — no issues detected',
      b: `tick=${State.tick}  sent=${State.stats.sent}  rcvd=${State.stats.rcvd}  mode=${State.appMode}`,
      r: ''
    });
  }

  const panel = document.getElementById('issues');
  if (!panel) return;
  panel.innerHTML = issues.map(i => `
    <div class="issue ${i.c}">
      <div class="issue-hd">${i.h}</div>
      <div class="issue-bd">${i.b}</div>
      ${i.r ? `<div class="issue-reason">→ ${i.r}</div>` : ''}
    </div>`).join('');
}

/* ── STATS ── */
function renderStats() {
  const el = id => document.getElementById(id);
  if (el('s-sent'))  el('s-sent').textContent  = State.stats.sent;
  if (el('s-rcvd'))  el('s-rcvd').textContent  = State.stats.rcvd;
  if (el('s-drop'))  el('s-drop').textContent  = State.stats.drop;
  const r = State.stats.lats.slice(-10);
  if (el('s-lat')) el('s-lat').textContent = r.length ? Math.round(r.reduce((a,b)=>a+b,0)/r.length)+'ms' : '—';
  if (el('diag-tick')) el('diag-tick').textContent = 'T' + String(State.tick).padStart(4,'0');
}

/* ── TIMELINE ── */
function renderTimeline() {
  const c = document.getElementById('tl-container');
  if (!c) return;
  c.innerHTML = '';
  State.procs.forEach(p => {
    const row = document.createElement('div'); row.className = 'tl-row';
    const lbl = document.createElement('div'); lbl.className = 'tl-name'; lbl.textContent = p.name;
    const bar = document.createElement('div'); bar.className = 'tl-bar';
    (State.timeline[p.id] || []).forEach(s => {
      const seg = document.createElement('div'); seg.className = 'tl-seg seg-' + s; bar.appendChild(seg);
    });
    row.appendChild(lbl); row.appendChild(bar); c.appendChild(row);
  });
}

/* ── RENDER ALL ── */
function renderAll() {
  renderProcs(); renderStats(); renderIssues(); renderTimeline();
  if (State.mode === 'pipe') renderPipe();
  if (State.mode === 'msgq') renderMsgQ();
  if (State.mode === 'shm')  renderSHM();
}

/* ── LOG ── */
function addLog(type, msg) {
  const wrap = document.getElementById('log-wrap');
  if (!wrap) return;
  const el = document.createElement('div');
  el.className = 'log-entry log-' + type;
  el.innerHTML = `<span class="log-ts">T${String(State.tick).padStart(4,'0')}</span><span class="log-msg">${msg}</span>`;
  wrap.insertBefore(el, wrap.firstChild);
  if (++logCount > 150 && wrap.lastChild) { wrap.removeChild(wrap.lastChild); logCount--; }
}

function clearLog() {
  const w = document.getElementById('log-wrap');
  if (w) w.innerHTML = '';
  logCount = 0;
}
