/**
 * Factory Floor — live pipeline visualization.
 * Polls the API, animates task chips flowing generators -> queue -> pumps -> done,
 * and interpolates pump progress / countdowns smoothly between polls (rAF).
 */
const API = '/api';
const POLL_MS = 2500;
const MACHINE_IDS = ['POMPE-1', 'POMPE-2', 'POMPE-3', 'POMPE-4', 'POMPE-5'];

const pumpEls = {};          // id -> pump DOM node
const live = {};             // id -> { status, taskStarted, taskDuration, pauseRemaining, polledAt }
const prevStatus = {};       // id -> previous status
const prevTaskId = {};       // id -> previous task id
let lastQueueSize = 0;

// Display labels + hover help for backend status enums (kept in French in the data model).
const STATUS_LABEL = { DISPONIBLE: 'AVAILABLE', ASSIGNEE: 'RUNNING', PAUSE: 'COOLDOWN', ARRET: 'DOWN', IDLE: 'IDLE' };
const STATUS_FALLBACK = { DISPONIBLE: 'Idle', PAUSE: 'Cooldown', ARRET: 'Down', IDLE: 'Idle' };
const STATUS_HELP = {
    DISPONIBLE: 'Idle and ready — it will take the next queued task.',
    ASSIGNEE: 'Processing a task. The bar shows progress; the task auto-completes when it fills.',
    PAUSE: 'Cooldown — resting after stress/overload. It auto-resumes when the timer ends.',
    ARRET: 'Broken down — it stays down until you press Repair.',
    IDLE: 'Powered down to save energy after being idle.',
};

// ==================== TIME ====================
function updateTime() {
    const el = document.getElementById('current-date');
    if (el) el.textContent = new Date().toLocaleTimeString('fr-FR');
}
function fmt(sec) {
    sec = Math.max(0, Math.floor(sec));
    const m = Math.floor(sec / 60), s = sec % 60;
    return `${m}:${String(s).padStart(2, '0')}`;
}

// ==================== PUMP RENDERING ====================
function ensurePump(id) {
    if (pumpEls[id]) return pumpEls[id];
    const el = document.createElement('div');
    el.className = 'pump';
    el.dataset.id = id;
    el.innerHTML = `
        <div class="pump-top">
            <span class="pump-name">${id}</span>
            <span class="pump-state badge">—</span>
        </div>
        <div class="pump-core"><div class="gear">⚙️</div></div>
        <div class="pump-task">—</div>
        <div class="pump-bar"><div class="pump-bar-fill"></div></div>
        <div class="pump-foot">
            <span class="pump-time">—</span>
            <span class="pump-gauge">🌡️ <b class="pt">–</b>°</span>
            <span class="pump-gauge">⚡ <b class="pc">–</b>%</span>
        </div>
        <div class="pump-overlay"></div>`;
    pumpEls[id] = el;
    return el;
}

function renderPumps(machines) {
    const grid = document.getElementById('pumps');
    if (!grid) return;
    const byId = {};
    machines.forEach(m => (byId[m.id] = m));
    // keep a stable order
    const ids = MACHINE_IDS.filter(id => byId[id]).concat(
        machines.map(m => m.id).filter(id => !MACHINE_IDS.includes(id))
    );

    if (grid.querySelector('.loading')) grid.innerHTML = '';

    ids.forEach(id => {
        const m = byId[id];
        const el = ensurePump(id);
        if (el.parentNode !== grid) grid.appendChild(el);

        const status = (m.status || 'DISPONIBLE');
        el.className = 'pump ' + status.toLowerCase();
        el.title = `${id} — ${STATUS_HELP[status] || ''}`;
        el.querySelector('.pump-state').textContent = STATUS_LABEL[status] || status;
        el.querySelector('.pump-state').className = 'pump-state badge ' + status.toLowerCase();
        el.querySelector('.pt').textContent = (m.temperature ?? 0).toFixed(0);
        el.querySelector('.pc').textContent = (m.charge ?? 0).toFixed(0);

        const taskEl = el.querySelector('.pump-task');
        if (status === 'ASSIGNEE' && m.task) {
            taskEl.innerHTML = `<b>${m.task}</b>${m.product ? ' · ' + m.product : ''}`;
        } else {
            taskEl.textContent = STATUS_FALLBACK[status] || '—';
        }

        // overlay (repair button when down)
        const ov = el.querySelector('.pump-overlay');
        if (status === 'ARRET') {
            ov.innerHTML = `<button class="btn-repair" onclick="repairMachine('${id}')">🔧 Repair</button>`;
        } else {
            ov.innerHTML = '';
        }

        // store live timing for the rAF loop
        live[id] = {
            status,
            taskStarted: m.task_started || null,
            taskDuration: m.task_duration || null,
            progress: m.progress,
            pauseRemaining: (status === 'PAUSE' && typeof m.time_remaining === 'number') ? m.time_remaining : null,
            polledAt: Date.now() / 1000,
        };
    });
}

// ==================== rAF: smooth bars + countdowns ====================
function animate() {
    const now = Date.now() / 1000;
    MACHINE_IDS.forEach(id => {
        const el = pumpEls[id], s = live[id];
        if (!el || !s) return;
        const fill = el.querySelector('.pump-bar-fill');
        const timeEl = el.querySelector('.pump-time');

        if (s.status === 'ASSIGNEE' && s.taskStarted && s.taskDuration) {
            const p = Math.max(0, Math.min(1, (now - s.taskStarted) / s.taskDuration));
            fill.style.width = (p * 100).toFixed(1) + '%';
            timeEl.textContent = '⏳ ' + fmt(s.taskDuration - (now - s.taskStarted));
        } else if (s.status === 'PAUSE' && s.pauseRemaining != null) {
            fill.style.width = '100%';
            const rem = s.pauseRemaining - (now - s.polledAt);
            timeEl.textContent = '❄️ cooldown ' + fmt(rem);
        } else if (s.status === 'ARRET') {
            fill.style.width = '100%';
            timeEl.textContent = 'down';
        } else {
            fill.style.width = '0%';
            timeEl.textContent = '—';
        }
    });
    requestAnimationFrame(animate);
}

// ==================== FLYING CHIPS ====================
function centerOf(el) {
    const r = el.getBoundingClientRect();
    return { x: r.left + r.width / 2, y: r.top + r.height / 2 };
}
function flyChip(fromEl, toEl, label, cls) {
    if (!fromEl || !toEl) return;
    const layer = document.getElementById('fly-layer');
    const a = centerOf(fromEl), b = centerOf(toEl);
    const chip = document.createElement('div');
    chip.className = 'chip ' + (cls || '');
    chip.textContent = label || '●';
    chip.style.left = a.x + 'px';
    chip.style.top = a.y + 'px';
    layer.appendChild(chip);
    requestAnimationFrame(() => {
        chip.style.transform = `translate(${b.x - a.x}px, ${b.y - a.y}px) scale(.65)`;
        chip.style.opacity = '0.15';
    });
    setTimeout(() => chip.remove(), 950);
}

// ==================== QUEUE / GENERATORS / KPIs ====================
function renderQueue(size) {
    const stack = document.getElementById('queue-stack');
    const count = document.getElementById('queue-count');
    if (count) count.textContent = size;
    if (!stack) return;
    const show = Math.min(6, size);
    let html = '';
    for (let i = 0; i < show; i++) html += '<div class="qchip"></div>';
    if (size > show) html += `<div class="qmore">+${size - show}</div>`;
    stack.innerHTML = html || '<div class="qempty">empty</div>';
}

function renderKPIs(stats) {
    const set = (id, v) => { const e = document.getElementById(id); if (e) e.textContent = v; };
    if (stats.machines) {
        set('stat-disponible', stats.machines.disponible || 0);
        set('stat-assignee', stats.machines.assignee || 0);
        set('stat-pause', stats.machines.pause || 0);
        set('stat-arret', stats.machines.arret || 0);
    }
    if (stats.tasks) {
        set('stat-completed', stats.tasks.completed || 0);
        set('queue-size', stats.tasks.in_queue || 0);
        set('done-count', stats.tasks.completed || 0);
    }
}

// ==================== POLLING ====================
async function poll() {
    try {
        const [mRes, sRes, tRes, simRes] = await Promise.all([
            fetch(`${API}/machines`).then(r => r.json()),
            fetch(`${API}/stats`).then(r => r.json()),
            fetch(`${API}/tasks`).then(r => r.json()),
            fetch(`${API}/simulators/status`).then(r => r.json()).catch(() => ({})),
        ]);

        const machines = mRes.machines || [];
        renderPumps(machines);
        renderKPIs(sRes);
        renderQueue((tRes && tRes.queue_size) || (sRes.tasks && sRes.tasks.in_queue) || 0);

        // generator on/off from the REAL simulator state (so Stop reflects at once)
        const prodOn = !!(simRes.production && simRes.production.running);
        const sensOn = !!(simRes.capteurs && simRes.capteurs.running);
        setGen('gen-production', 'production-status', 'gen-production-rate', prodOn, prodOn ? `1 task / ${simRes.production.frequency}s` : 'stopped');
        setGen('gen-sensors', 'capteurs-status', 'gen-sensors-rate', sensOn, sensOn ? `1 read / ${simRes.capteurs.frequency}s` : 'stopped');
        const lane = document.querySelector('.stage-lane');
        if (lane) lane.classList.toggle('flowing', prodOn);

        // transition detection -> animate chips
        const queueEl = document.querySelector('.col-queue');
        const doneEl = document.getElementById('done-bin');
        machines.forEach(m => {
            const id = m.id, st = m.status, ps = prevStatus[id];
            const pump = pumpEls[id];
            if (ps === 'ASSIGNEE' && st !== 'ASSIGNEE' && prevTaskId[id]) {
                flyChip(pump, doneEl, '✓', 'done');           // finished -> shipped out
            }
            if (st === 'ASSIGNEE' && m.task && m.task !== prevTaskId[id]) {
                flyChip(queueEl, pump, '', 'task');            // pulled from queue
            }
            if (st === 'ARRET' && ps && ps !== 'ARRET' && pump) {
                pump.classList.add('flash-break');
                setTimeout(() => pump.classList.remove('flash-break'), 1200);
            }
            prevStatus[id] = st;
            prevTaskId[id] = (st === 'ASSIGNEE') ? m.task : null;
        });

        // generators "emit" chips into the queue when production grew
        const qsize = (tRes && tRes.queue_size) || 0;
        if (qsize > lastQueueSize) {
            const gen = document.getElementById('gen-production');
            const q = document.querySelector('.col-queue');
            const n = Math.min(3, qsize - lastQueueSize);
            for (let i = 0; i < n; i++) setTimeout(() => flyChip(gen, q, '', 'task'), i * 180);
        }
        lastQueueSize = qsize;
    } catch (e) {
        console.error('poll error', e);
    }
}

// ==================== GENERATOR STATUS (from real data flow) ====================
function setGen(nodeId, badgeId, rateId, active, verb) {
    const node = document.getElementById(nodeId);
    if (node) node.classList.toggle('active', active);
    const badge = document.getElementById(badgeId);
    if (badge) {
        badge.textContent = active ? '🟢 Running' : '⚪ Idle';
        badge.className = 'simulator-status ' + (active ? 'running' : 'stopped');
    }
    const rate = document.getElementById(rateId);
    if (rate) rate.textContent = active ? verb : 'idle';
}

// ==================== TOAST ====================
function toast(msg, ok = true) {
    const t = document.createElement('div');
    t.className = 'toast ' + (ok ? 'ok' : 'err');
    t.textContent = msg;
    document.body.appendChild(t);
    requestAnimationFrame(() => t.classList.add('show'));
    setTimeout(() => { t.classList.remove('show'); setTimeout(() => t.remove(), 300); }, 2600);
}

// ==================== SIMULATOR CONTROLS ====================
async function post(path) { return fetch(`${API}${path}`, { method: 'POST' }).then(r => r.json()).catch(() => ({})); }
function feedback(r, fallback) { toast(r.message || r.detail || fallback, !!r.message); }
function laneFlow(on) { const l = document.querySelector('.stage-lane'); if (l) l.classList.toggle('flowing', on); }
async function startCapteursSimulator() { setGen('gen-sensors', 'capteurs-status', 'gen-sensors-rate', true, 'starting…'); feedback(await post('/simulators/capteurs/start'), 'Sensors'); }
async function stopCapteursSimulator() { setGen('gen-sensors', 'capteurs-status', 'gen-sensors-rate', false, 'stopped'); feedback(await post('/simulators/capteurs/stop'), 'Sensors'); }
async function startProductionSimulator() { setGen('gen-production', 'production-status', 'gen-production-rate', true, 'starting…'); laneFlow(true); feedback(await post('/simulators/production/start'), 'Production'); }
async function stopProductionSimulator() { setGen('gen-production', 'production-status', 'gen-production-rate', false, 'stopped'); laneFlow(false); feedback(await post('/simulators/production/stop'), 'Production'); }

function updateCapteursFrenquency(v) {
    const e = document.getElementById('capteurs-frequency-value'); if (e) e.textContent = `${v}s`;
    fetch(`${API}/simulators/capteurs/config`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ frequency: parseInt(v) }) }).catch(() => {});
}
function updateCapteursErrorRate(v) {
    const e = document.getElementById('capteurs-error-rate-value'); if (e) e.textContent = `${v}%`;
    fetch(`${API}/simulators/capteurs/config`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ error_rate: parseFloat(v) / 100 }) }).catch(() => {});
}
function updateProductionFrequency(v) {
    const e = document.getElementById('production-frequency-value'); if (e) e.textContent = `${v}s`;
    fetch(`${API}/simulators/production/config`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ frequency: parseInt(v) }) }).catch(() => {});
}
async function forceError() {
    const machineId = document.getElementById('force-error-machine').value;
    const r = await fetch(`${API}/simulators/capteurs/force-error`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ machine_id: machineId }) }).then(r => r.json()).catch(() => ({}));
    feedback(r, 'Breakdown forced');
}
async function repairMachine(id) {
    const r = await fetch(`${API}/machines/${id}/repair`, { method: 'POST' }).then(r => r.json()).catch(() => ({}));
    feedback(r, 'Repair sent');
    poll();
}

// ==================== INIT ====================
function wireGrafana() {
    const host = `http://${location.hostname || 'localhost'}:3000`;
    const frame = document.getElementById('grafana-frame');
    if (frame) frame.src = `${host}/d/factory-main?orgId=1&kiosk&theme=dark&refresh=10s&from=now-30m&to=now`;
    document.querySelectorAll('#grafana-link, #grafana-link-2').forEach(a => { a.href = `${host}/d/factory-main?orgId=1`; });
}

document.addEventListener('DOMContentLoaded', () => {
    wireGrafana();
    updateTime();
    poll();
    requestAnimationFrame(animate);
    setInterval(updateTime, 1000);
    setInterval(poll, POLL_MS);
});
