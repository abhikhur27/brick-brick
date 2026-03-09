# Brick Brick GitHub Pages Site

## Files
- `index.html` main marketing site
- `styles.css` main site styling
- `main.js` nav and contact form logic
- `portal.html` employee portal route
- `portal.css` employee portal styling
- `portal.js` Firebase auth + Firestore profile rendering
- `firebase-config.example.js` starter config; copy to `firebase-config.js`
- `firestore.rules` Firestore access rules example
- `.nojekyll` disables Jekyll processing on GitHub Pages
- `assets/placeholder-*.svg` profile placeholders

## Deploy on GitHub Pages
1. Push all files to the repository root.
2. In GitHub: Settings → Pages.
3. Under Build and deployment, choose **Deploy from a branch**.
4. Select the `main` branch and `/ (root)`.
5. Save.

## Portal URL
If your Pages URL is:
`https://USERNAME.github.io/REPO/`
then the portal will be at:
`https://USERNAME.github.io/REPO/portal.html`

## Firebase setup
1. Create a Firebase project.
2. Enable Authentication → Email/Password.
3. Create users manually in Authentication.
4. Create Firestore database.
5. Copy `firebase-config.example.js` to `firebase-config.js` and paste your web app config.
6. Add employee docs to the `employees` collection.
7. Apply `firestore.rules` in Firestore Rules.

Example employee document:
```json
{
  "name": "Athan",
  "role": "Operations",
  "bio": "Leads outreach, deal flow, and execution.",
  "email": "athan@brick-brick.org",
  "image": "assets/placeholder-athan.svg"
}
```

## Important note
The portal page HTML itself is still publicly hosted because GitHub Pages is a static hosting service. The protected part is the data and the signed-in experience, enforced by Firebase Authentication and Firestore rules.
