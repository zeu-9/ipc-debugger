/* state.js — global simulation state (v4) */
const NAMES = ['P0','P1','P2','P3'];
const PCOLS = ['#5ba4f5','#3ecf8e','#f5a623','#c084fc'];
const PBGS  = ['rgba(91,164,245,.20)','rgba(62,207,142,.20)','rgba(245,166,35,.20)','rgba(192,132,252,.20)'];

const State = {
  running: false, timer: null, tick: 0, speed: 700,
  mode: 'pipe', numProcs: 4,
  appMode: 'auto',   // 'auto' | 'manual'

  /* manual-only fault flags */
  dlActive: false, bnActive: false, raceActive: false,

  /* processes */
  procs: [], timeline: {},

  /* IPC channels */
  pipe: { buf: [], cap: 12 },
  msgq: { q: [], cap: 8, sent: 0, rcvd: 0 },
  shm:  { segs: [] },

  /* sync primitives: [{ id, type:'mutex'|'semaphore', state:'free'|'acquired'|'waiting'|'deadlock', owner, count, max }] */
  primitives: [],
  semaphoreMax: 2,

  /* throughput tracking */
  throughput: {
    producerTicks: 0,   // ticks writer was active
    consumerTicks: 0,   // ticks reader was active
    windowSent: [],     // rolling window of sent timestamps
    windowRcvd: [],     // rolling window of rcvd timestamps
    WINDOW: 10,
  },

  /* auto-detection history for algorithms */
  waitHistory: {},    // { pid: consecutive_wait_ticks }
  blockHistory: {},   // { pid: consecutive_block_ticks }
  resourceHolding: {},// { pid: [resources held] }
  resourceWaiting: {},// { pid: resource waiting for }
  dropHistory: [],    // rolling window of drop events

  /* stats */
  stats: { sent: 0, rcvd: 0, drop: 0, lats: [] },

  reset() {
    clearTimeout(this.timer);
    this.running = false; this.tick = 0;
    this.dlActive = false; this.bnActive = false; this.raceActive = false;
    this.pipe  = { buf: [], cap: 12 };
    this.msgq  = { q: [], cap: 8, sent: 0, rcvd: 0 };
    this.stats = { sent: 0, rcvd: 0, drop: 0, lats: [] };
    this.throughput = { producerTicks:0, consumerTicks:0, windowSent:[], windowRcvd:[], WINDOW:10 };
    this.waitHistory = {}; this.blockHistory = {};
    this.resourceHolding = {}; this.resourceWaiting = {};
    this.dropHistory = [];
    this.initProcs();
    this.initPrimitives();
  },

  initProcs() {
    this.procs = []; this.timeline = {};
    for (let i = 0; i < this.numProcs; i++) {
      this.procs.push({ id: i, name: NAMES[i], state: 'idle',
        role: i === 0 ? 'writer' : i === this.numProcs - 1 ? 'reader' : 'relay',
        pid: 1000 + Math.floor(Math.random() * 9000),
        col: PCOLS[i], bg: PBGS[i] });
      this.timeline[i] = [];
      this.waitHistory[i] = 0;
      this.blockHistory[i] = 0;
      this.resourceHolding[i] = [];
      this.resourceWaiting[i] = null;
    }
    this.shm.segs = Array.from({ length: 32 }, (_, i) =>
      ({ id: i, state: 'free', owner: null, val: null }));
  },

  initPrimitives() {
    this.primitives = [];
    const max = this.semaphoreMax;
    // Always create: 1 mutex for write-lock, 1 mutex for read-lock, semaphore
    this.primitives.push({ id: 'mtx_write', type: 'mutex',     label: 'mutex_W', state: 'free', owner: null, count: 1, max: 1 });
    this.primitives.push({ id: 'mtx_read',  type: 'mutex',     label: 'mutex_R', state: 'free', owner: null, count: 1, max: 1 });
    this.primitives.push({ id: 'sem_empty', type: 'semaphore', label: `sem(${max})`, state: 'free', owner: null, count: max, max });
    this.primitives.push({ id: 'sem_full',  type: 'semaphore', label: 'sem_full', state: 'free', owner: null, count: 0, max });
  },

  pushTL() {
    this.procs.forEach(p => {
      this.timeline[p.id].push(p.state);
      if (this.timeline[p.id].length > 20) this.timeline[p.id].shift();
      // track consecutive waits/blocks for deadlock detection
      if (p.state === 'wait')  this.waitHistory[p.id]  = (this.waitHistory[p.id]  || 0) + 1;
      else                     this.waitHistory[p.id]  = 0;
      if (p.state === 'block') this.blockHistory[p.id] = (this.blockHistory[p.id] || 0) + 1;
      else                     this.blockHistory[p.id] = 0;
    });
    // Update throughput window
    if (this.getW()?.state === 'run') {
      this.throughput.producerTicks++;
      this.throughput.windowSent.push(this.tick);
    }
    if (this.getR()?.state === 'run') {
      this.throughput.consumerTicks++;
      this.throughput.windowRcvd.push(this.tick);
    }
    // Keep rolling window
    const W = this.throughput.WINDOW;
    if (this.throughput.windowSent.length > W) this.throughput.windowSent.shift();
    if (this.throughput.windowRcvd.length > W) this.throughput.windowRcvd.shift();
  },

  getW()   { return this.procs.find(p => p.role === 'writer'); },
  getR()   { return this.procs.find(p => p.role === 'reader'); },
  getRel() { return this.procs.filter(p => p.role === 'relay'); },
  getPrim(id) { return this.primitives.find(p => p.id === id); },
};
