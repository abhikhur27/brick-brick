import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.5/firebase-app.js";
import { getAuth, signInWithEmailAndPassword, signOut, onAuthStateChanged } from "https://www.gstatic.com/firebasejs/10.12.5/firebase-auth.js";
import { getFirestore, collection, addDoc, getDocs, deleteDoc, doc, updateDoc, query, orderBy } from "https://www.gstatic.com/firebasejs/10.12.5/firebase-firestore.js";
import { firebaseConfig } from "./firebase-config.js";

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);

const emailInput = document.getElementById('emailInput');
const passwordInput = document.getElementById('passwordInput');
const loginError = document.getElementById('loginError');
const loginScreen = document.getElementById('loginScreen');
const appShell = document.getElementById('app');
const signedInAs = document.getElementById('signedInAs');
const refreshBtn = document.getElementById('refreshBtn');
const addBtn = document.getElementById('addBtn');
const modalOverlay = document.getElementById('modalOverlay');
const modalTitle = document.getElementById('modalTitle');
const modalBody = document.getElementById('modalBody');
const modalSave = document.getElementById('modalSave');
const pageTitle = document.getElementById('pageTitle');
const pageSubtitle = document.getElementById('pageSubtitle');

const COLS = ['Leads', 'In Conversation', 'Proposal Sent', 'Closed'];
let currentPage = 'dashboard';
let state = { pipeline: [], tasks: [], decisions: [] };
let modalMode = 'pipeline';
let modalContext = '';

const SUBTITLES = {
  dashboard: 'Shared operating workspace for Brick Brick.',
  pipeline: 'Track opportunities from lead to close.',
  tasks: 'Keep execution visible and assigned.',
  decisions: 'Capture what the team decided and why.'
};

window.doLogin = async function () {
  loginError.textContent = '';
  try {
    await signInWithEmailAndPassword(auth, emailInput.value.trim(), passwordInput.value);
  } catch (err) {
    loginError.textContent = err.message;
  }
};

window.doLogout = async function () {
  await signOut(auth);
};

window.closeModal = function () {
  modalOverlay.classList.remove('open');
};

modalOverlay.addEventListener('click', (e) => {
  if (e.target === modalOverlay) closeModal();
});

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') closeModal();
});

onAuthStateChanged(auth, async (user) => {
  if (user) {
    loginScreen.style.display = 'none';
    appShell.classList.add('visible');
    signedInAs.textContent = user.email;
    await loadAll();
  } else {
    appShell.classList.remove('visible');
    loginScreen.style.display = 'flex';
    signedInAs.textContent = '';
    passwordInput.value = '';
  }
});

refreshBtn.addEventListener('click', () => loadAll());
addBtn.addEventListener('click', () => openAddModal(currentPage === 'dashboard' ? 'pipeline' : currentPage));
modalSave.addEventListener('click', saveModal);

document.querySelectorAll('.nav-item').forEach((item) => {
  item.addEventListener('click', () => showPage(item.dataset.page));
});
document.querySelectorAll('[data-jump]').forEach((btn) => {
  btn.addEventListener('click', () => showPage(btn.dataset.jump));
});

function uid() {
  return '_' + Math.random().toString(36).slice(2, 11);
}

async function loadCollection(name, orderField = null) {
  const ref = collection(db, name);
  const q = orderField ? query(ref, orderBy(orderField, 'asc')) : ref;
  const snap = await getDocs(q);
  return snap.docs.map((d) => ({ id: d.id, ...d.data() }));
}

async function loadAll() {
  try {
    const [pipeline, tasks, decisions] = await Promise.all([
      loadCollection('pipeline'),
      loadCollection('tasks'),
      loadCollection('decisions')
    ]);
    state = { pipeline, tasks, decisions };
    renderAll();
  } catch (err) {
    console.error(err);
    loginError.textContent = err.message;
  }
}

function renderAll() {
  renderDashboard();
  renderPipeline();
  renderTasks();
  renderDecisions();
}

function renderDashboard() {
  const openLeads = state.pipeline.filter((x) => x.status !== 'Closed').length;
  const closedDeals = state.pipeline.filter((x) => x.status === 'Closed').length;
  const openTasks = state.tasks.filter((x) => !x.done).length;
  document.getElementById('statOpenLeads').textContent = openLeads;
  document.getElementById('statClosedDeals').textContent = closedDeals;
  document.getElementById('statOpenTasks').textContent = openTasks;
  document.getElementById('statDecisions').textContent = state.decisions.length;

  const pipeWrap = document.getElementById('dashboardPipeline');
  const taskWrap = document.getElementById('dashboardTasks');
  const recentPipeline = [...state.pipeline].slice(0, 4);
  const priorityTasks = [...state.tasks]
    .sort((a, b) => (a.done === b.done ? priorityScore(a.priority) - priorityScore(b.priority) : a.done ? 1 : -1))
    .slice(0, 4);

  pipeWrap.innerHTML = recentPipeline.length ? recentPipeline.map(card => `
    <div class="mini-item">
      <div class="mini-item-title">${escapeHtml(card.title || 'Untitled')}</div>
      <div class="mini-item-meta">${escapeHtml(card.service || 'Other')} · ${escapeHtml(card.status || 'Leads')}</div>
    </div>`).join('') : '<div class="empty">No leads yet. Click + Add to create one.</div>';

  taskWrap.innerHTML = priorityTasks.length ? priorityTasks.map(task => `
    <div class="mini-item">
      <div class="mini-item-title">${escapeHtml(task.name || 'Untitled task')}</div>
      <div class="mini-item-meta">${escapeHtml(task.owner || 'Team')} · ${escapeHtml(task.priority || 'Mid')}</div>
    </div>`).join('') : '<div class="empty">No tasks yet. Add the first task for the team.</div>';
}

function renderPipeline() {
  const board = document.getElementById('kanbanBoard');
  board.innerHTML = '';
  COLS.forEach(col => {
    const cards = state.pipeline.filter(c => (c.status || 'Leads') === col);
    const colEl = document.createElement('div');
    colEl.className = 'kanban-col';
    colEl.innerHTML = `
      <div class="col-header">
        <span class="col-title">${col}</span>
        <span class="col-count">${cards.length}</span>
      </div>
      <div id="col-${col.replace(/\s/g, '_')}"></div>
      <button class="add-card">+ Add</button>`;
    colEl.querySelector('.add-card').addEventListener('click', () => openAddModal('pipeline', col));
    board.appendChild(colEl);
    const container = colEl.querySelector(`#col-${col.replace(/\s/g, '_')}`);
    if (!cards.length) {
      container.innerHTML = '<div class="empty-state">No items in this stage yet.</div>';
    }
    cards.forEach(card => {
      const tagClass = card.service === 'AI Workflow' ? 'tag-ai' : card.service === 'Website Build' ? 'tag-web' : 'tag-other';
      const moves = COLS.filter(c => c !== card.status).map(c => `<button class="card-btn" data-move="${c}">${c}</button>`).join('');
      const cardEl = document.createElement('div');
      cardEl.className = 'card';
      cardEl.innerHTML = `
        <div class="card-title">${escapeHtml(card.title || 'Untitled')}</div>
        <div class="card-meta">${escapeHtml(card.note || '')}</div>
        <span class="card-tag ${tagClass}">${escapeHtml(card.service || 'Other')}</span>
        <div class="card-actions">${moves}<button class="card-btn delete">✕</button></div>`;
      cardEl.querySelectorAll('[data-move]').forEach(btn => btn.addEventListener('click', async () => {
        await updateDoc(doc(db, 'pipeline', card.id), { status: btn.dataset.move });
        await loadAll();
      }));
      cardEl.querySelector('.delete').addEventListener('click', async () => {
        await deleteDoc(doc(db, 'pipeline', card.id));
        await loadAll();
      });
      container.appendChild(cardEl);
    });
  });
}

function renderTasks() {
  const tbody = document.getElementById('tasksBody');
  tbody.innerHTML = '';
  const sorted = [...state.tasks].sort((a, b) => {
    if (!!a.done !== !!b.done) return a.done ? 1 : -1;
    return (a.due || '9999-99-99').localeCompare(b.due || '9999-99-99');
  });
  if (!sorted.length) {
    tbody.innerHTML = '<tr><td colspan="6"><div class="empty-state">No tasks yet. Add a task for the team.</div></td></tr>';
    return;
  }
  sorted.forEach(task => {
    const tr = document.createElement('tr');
    if (task.done) tr.classList.add('task-done-row');
    const isOverdue = !task.done && task.due && new Date(task.due) < new Date(new Date().toDateString());
    const pClass = task.priority === 'High' ? 'p-high' : task.priority === 'Mid' ? 'p-mid' : 'p-low';
    const initials = (task.owner || 'Team').split(' ').map(w => w[0]).join('').toUpperCase().slice(0, 2);
    tr.innerHTML = `
      <td><div class="checkbox ${task.done ? 'checked' : ''}"></div></td>
      <td class="task-name">${escapeHtml(task.name || 'Untitled task')}</td>
      <td><span class="owner-chip"><span class="avatar">${initials}</span>${escapeHtml(task.owner || 'Team')}</span></td>
      <td><span class="date-text ${isOverdue ? 'date-overdue' : ''}">${formatDate(task.due)}</span></td>
      <td><span class="priority ${pClass}">${escapeHtml(task.priority || 'Mid')}</span></td>
      <td><button class="card-btn delete">✕</button></td>`;
    tr.querySelector('.checkbox').addEventListener('click', async () => {
      await updateDoc(doc(db, 'tasks', task.id), { done: !task.done });
      await loadAll();
    });
    tr.querySelector('.delete').addEventListener('click', async () => {
      await deleteDoc(doc(db, 'tasks', task.id));
      await loadAll();
    });
    tbody.appendChild(tr);
  });
}

function renderDecisions() {
  const log = document.getElementById('decisionsLog');
  log.innerHTML = '';
  if (!state.decisions.length) {
    log.innerHTML = '<div class="empty-state">No meetings logged yet. Add your first decision entry.</div>';
    return;
  }
  state.decisions.forEach(entry => {
    const el = document.createElement('div');
    el.className = 'decision-entry';
    const items = Array.isArray(entry.items) ? entry.items : [];
    el.innerHTML = `
      <div class="decision-date">
        <span>${escapeHtml(entry.date || 'Undated')}</span>
        <button class="delete-entry-btn delete">Delete</button>
      </div>
      ${items.map(item => `
        <div class="decision-item">
          <div class="decision-text">${escapeHtml(item.text || '')}</div>
          <div class="decision-owner">${escapeHtml(item.owner || '')}</div>
        </div>`).join('')}`;
    el.querySelector('.delete').addEventListener('click', async () => {
      await deleteDoc(doc(db, 'decisions', entry.id));
      await loadAll();
    });
    log.appendChild(el);
  });
}

function priorityScore(priority) {
  return priority === 'High' ? 0 : priority === 'Mid' ? 1 : 2;
}

function formatDate(str) {
  if (!str) return '—';
  const d = new Date(`${str}T00:00:00`);
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

function showPage(name) {
  currentPage = name;
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.getElementById(`page-${name}`).classList.add('active');
  document.querySelector(`.nav-item[data-page="${name}"]`)?.classList.add('active');
  pageTitle.textContent = name.toUpperCase();
  pageSubtitle.textContent = SUBTITLES[name] || '';
}

function openAddModal(type, context = '') {
  modalMode = type;
  modalContext = context;
  modalTitle.textContent = type === 'pipeline' ? 'ADD LEAD' : type === 'tasks' ? 'ADD TASK' : 'LOG DECISION';
  modalBody.innerHTML = getModalForm(type, context);
  modalOverlay.classList.add('open');
}

function getModalForm(mode, context) {
  if (mode === 'pipeline') {
    return `
      <div class="form-group"><label class="form-label">Lead / Client</label><input class="form-input" id="f_title" placeholder="e.g. Local gym chain"></div>
      <div class="form-group"><label class="form-label">Service Type</label><select class="form-select" id="f_service"><option>AI Workflow</option><option>Website Build</option><option>Internal Tool</option><option>Other</option></select></div>
      <div class="form-group"><label class="form-label">Stage</label><select class="form-select" id="f_status">${COLS.map(c => `<option ${c===context?'selected':''}>${c}</option>`).join('')}</select></div>
      <div class="form-group"><label class="form-label">Notes</label><input class="form-input" id="f_note" placeholder="What do they need?"></div>`;
  }
  if (mode === 'tasks') {
    return `
      <div class="form-group"><label class="form-label">Task</label><input class="form-input" id="f_name" placeholder="What needs to get done?"></div>
      <div class="form-group"><label class="form-label">Owner</label><input class="form-input" id="f_owner" placeholder="Athan / Chris / Team"></div>
      <div class="form-group"><label class="form-label">Due date</label><input class="form-input" type="date" id="f_due"></div>
      <div class="form-group"><label class="form-label">Priority</label><select class="form-select" id="f_priority"><option>High</option><option selected>Mid</option><option>Low</option></select></div>`;
  }
  return `
    <div class="form-group"><label class="form-label">Meeting title / date</label><input class="form-input" id="f_date" placeholder="e.g. March 10, 2026 — Weekly Sync"></div>
    <div class="form-group"><label class="form-label">Decision 1</label><input class="form-input" id="f_d1" placeholder="What was decided?"></div>
    <div class="form-group"><label class="form-label">Owner / action 1</label><input class="form-input" id="f_o1" placeholder="Who owns the next step?"></div>
    <div class="form-group"><label class="form-label">Decision 2</label><input class="form-input" id="f_d2"></div>
    <div class="form-group"><label class="form-label">Owner / action 2</label><input class="form-input" id="f_o2"></div>
    <div class="form-group"><label class="form-label">Decision 3</label><input class="form-input" id="f_d3"></div>
    <div class="form-group"><label class="form-label">Owner / action 3</label><input class="form-input" id="f_o3"></div>`;
}

async function saveModal() {
  if (modalMode === 'pipeline') {
    const title = document.getElementById('f_title').value.trim();
    if (!title) return;
    await addDoc(collection(db, 'pipeline'), {
      title,
      service: document.getElementById('f_service').value,
      status: document.getElementById('f_status').value,
      note: document.getElementById('f_note').value.trim() || ''
    });
  } else if (modalMode === 'tasks') {
    const name = document.getElementById('f_name').value.trim();
    if (!name) return;
    await addDoc(collection(db, 'tasks'), {
      name,
      owner: document.getElementById('f_owner').value.trim() || 'Team',
      due: document.getElementById('f_due').value || '',
      priority: document.getElementById('f_priority').value,
      done: false
    });
  } else {
    const date = document.getElementById('f_date').value.trim();
    if (!date) return;
    const items = [1,2,3].map(i => ({
      text: document.getElementById(`f_d${i}`).value.trim(),
      owner: document.getElementById(`f_o${i}`).value.trim()
    })).filter(item => item.text);
    if (!items.length) return;
    await addDoc(collection(db, 'decisions'), { date, items });
  }
  closeModal();
  await loadAll();
}

function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, (m) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]));
}
