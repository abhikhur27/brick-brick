# BrickBrick Firebase Site

## Included
- Public marketing site at `/`
- Team portal at `/portal.html`
- Client panel at `/client.html`
- Firebase Auth email/password login
- Firestore-backed pipeline, tasks, decisions, clients, requests, and login provisioning

## Spark-Safe Architecture
- Hosting: Firebase Hosting (static site)
- Database: Cloud Firestore
- Auth: Firebase Authentication (Email/Password)
- No Cloud Functions
- No Admin SDK in frontend
- No Secret Manager or paid Google Cloud dependencies

## User Provisioning Model
- `clients` holds operational client records.
- `users/{authUid}` is the runtime access mapping used after login.
- `login_provisioning/{email}` is managed by super admins to stage access safely on Spark.

### Why this model
- Spark/free tier cannot safely perform privileged Auth user management from frontend.
- Super admins manage provisioning + profile access in-app.
- Actual Auth account creation/deletion still happens in Firebase Console.

## Deploy
1. `firebase login`
2. `firebase use --add` (if needed)
3. `firebase deploy --only hosting,firestore:rules,firestore:indexes`

## First-Time Setup
1. Enable `Authentication -> Sign-in method -> Email/Password`.
2. Create Firestore database (Production or Test mode as desired).
3. Deploy rules:
   - `firebase deploy --only firestore:rules`
4. Create first super admin:
   - In Firebase Auth, create an Email/Password user.
   - In Firestore, add `users/{authUid}` with:
     - `role: "super_admin"`
     - `clientId: ""`
     - `email: "<same email>"`
     - `disabled: false`
     - `createdAt` / `updatedAt` timestamps

## Ongoing User Management (Spark-safe)
1. Super admin signs into `/portal.html`.
2. Use **Login Provisioning** section to create/update client/admin/super admin provisioning records.
3. Create corresponding Auth user in Firebase Console (same email as provisioning record).
4. User signs in:
   - Client panel auto-creates `users/{uid}` when linked client/provision matches.
   - Team portal auto-creates `users/{uid}` for provisioned admin/super admin.
5. Password reset:
   - Use in-app **Send Reset Link** button (super admin view).
6. Revoke access:
   - Use in-app **Revoke Access** (disables provisioning and removes linked profile).
   - Optionally disable/delete Auth user in Firebase Console.

## Outbound Lead Research Workflow (Spark-safe)
### Option A: CSV workflow (no credentials needed)
1. Run the city research script:
   - `python tools/osm_lead_research.py --city "Austin" --state "TX" --categories dentist,plumber,hvac --max-results-per-category 40`
2. This writes a CSV into `exports/`.
3. In `/portal.html` -> Pipeline -> Lead List Generator -> Public-Source Lead Research Intake:
   - click `Import CSV`
   - review rows
   - `Approve` then `Import`

### Option B: Direct Firestore ingest (still Spark-safe)
1. Export local env vars (PowerShell example):
   - `$env:BRICKBRICK_FIREBASE_PROJECT_ID="your-project-id"`
   - `$env:BRICKBRICK_FIREBASE_API_KEY="your-web-api-key"`
   - `$env:BRICKBRICK_FIREBASE_EMAIL="admin@yourdomain.com"`
   - `$env:BRICKBRICK_FIREBASE_PASSWORD="your-password"`
2. Run:
   - `python tools/osm_lead_research.py --city "Austin" --state "TX" --categories dentist,plumber,hvac --ingest-firestore`
3. Script signs in using Firebase Auth REST and writes staged rows to `lead_research_imports`.

### Notes
- Uses only public-source OpenStreetMap data with source attribution.
- No Cloud Functions, no external backend, no Blaze-only services.
- Team portal includes a `Research Source Quality Score` panel that ranks directories by conversion proxy (closed stage) and confidence.

## Core Collections
- `pipeline`
- `tasks`
- `decisions`
- `clients`
- `client_requests`
- `users`
- `login_provisioning`
- `contact_submissions`
- `lead_research_imports`
