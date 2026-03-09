import { auth, db } from './firebase-config.js';
import {
  signInWithEmailAndPassword,
  onAuthStateChanged,
  signOut
} from 'https://www.gstatic.com/firebasejs/10.12.5/firebase-auth.js';
import {
  collection,
  getDocs
} from 'https://www.gstatic.com/firebasejs/10.12.5/firebase-firestore.js';

const authPanel = document.getElementById('authPanel');
const portalContent = document.getElementById('portalContent');
const loginForm = document.getElementById('loginForm');
const authStatus = document.getElementById('authStatus');
const profilesGrid = document.getElementById('profilesGrid');
const logoutBtn = document.getElementById('logoutBtn');
const signedInAs = document.getElementById('signedInAs');

loginForm?.addEventListener('submit', async (event) => {
  event.preventDefault();
  const email = document.getElementById('loginEmail').value.trim();
  const password = document.getElementById('loginPassword').value.trim();

  try {
    authStatus.textContent = 'Signing in...';
    await signInWithEmailAndPassword(auth, email, password);
  } catch (error) {
    authStatus.textContent = error.message;
  }
});

logoutBtn?.addEventListener('click', async () => {
  await signOut(auth);
});

onAuthStateChanged(auth, async (user) => {
  if (!user) {
    authPanel.classList.remove('hidden');
    portalContent.classList.add('hidden');
    signedInAs.textContent = '';
    authStatus.textContent = 'Waiting for sign-in.';
    return;
  }

  authPanel.classList.add('hidden');
  portalContent.classList.remove('hidden');
  signedInAs.textContent = `Signed in as ${user.email}`;

  try {
    const snapshot = await getDocs(collection(db, 'employees'));
    const docs = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    renderProfiles(docs);
  } catch (error) {
    profilesGrid.innerHTML = `<article class="profile-card card"><h3>Could not load profiles</h3><p>${error.message}</p></article>`;
  }
});

function renderProfiles(profiles) {
  if (!profiles.length) {
    profilesGrid.innerHTML = `
      <article class="profile-card card">
        <h3>No employee documents yet</h3>
        <p>Add documents to the <strong>employees</strong> collection in Firestore, then reload this page.</p>
      </article>
    `;
    return;
  }

  profilesGrid.innerHTML = profiles.map(profile => `
    <article class="profile-card card">
      <img src="${escapeHtml(profile.image || 'assets/placeholder-abhi.svg')}" alt="${escapeHtml(profile.name || 'Employee')} profile image">
      <h3>${escapeHtml(profile.name || 'Unnamed employee')}</h3>
      <p class="role">${escapeHtml(profile.role || 'Role not set')}</p>
      <p>${escapeHtml(profile.bio || 'No bio yet.')}</p>
      ${profile.email ? `<p class="muted">${escapeHtml(profile.email)}</p>` : ''}
    </article>
  `).join('');
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}
