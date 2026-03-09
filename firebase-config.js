// Copy this file to firebase-config.js and replace the placeholder values.
// Do not commit secrets you do not want public. Firebase web config is not a password;
// access is enforced by Authentication and Firestore / Storage security rules.

import { initializeApp } from 'https://www.gstatic.com/firebasejs/10.12.5/firebase-app.js';
import { getAuth } from 'https://www.gstatic.com/firebasejs/10.12.5/firebase-auth.js';
import { getFirestore } from 'https://www.gstatic.com/firebasejs/10.12.5/firebase-firestore.js';

// For Firebase JS SDK v7.20.0 and later, measurementId is optional
const firebaseConfig = {
  apiKey: "AIzaSyD1BYQMWLvzbW8deoBhtSSd_qHPspMr4Yg",
  authDomain: "brickbrickholdingsllc.firebaseapp.com",
  projectId: "brickbrickholdingsllc",
  storageBucket: "brickbrickholdingsllc.firebasestorage.app",
  messagingSenderId: "365413140380",
  appId: "1:365413140380:web:500fb692158f4c3050ea59",
  measurementId: "G-4EFLQFLRHQ"
};

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);

export { app, auth, db };
