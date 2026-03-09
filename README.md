# Brick Brick Firebase Site

## Included
- Public marketing site at `/`
- Team portal at `/portal.html`
- Firebase Auth email/password login
- Shared Firestore collections for `pipeline`, `tasks`, and `decisions`

## Deploy
1. `firebase login`
2. `firebase use --add` if needed
3. `firebase deploy`

## Firebase collections
- `pipeline`
- `tasks`
- `decisions`

## First-time setup
- Enable Authentication -> Email/Password
- Create Firestore database
- Add team users in Authentication
- Deploy rules with `firebase deploy --only "firestore:rules"`
