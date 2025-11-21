# Embucket UI Lambda POC

Minimal Lambda-powered UI with Postgres integration for query history.

## Quick Start

1. **Setup:** See [ENV_SETUP_GUIDE.md](./docs/ENV_SETUP_GUIDE.md)
2. **Build:** `npm install && cd ui && npm install && cd .. && npm run build`
3. **Package:** `npm run package`
4. **Deploy:** Upload `function.zip` to Lambda

## Structure

```
poc-lambda-ui/
├── src/
│   ├── index.ts         # Lambda handler (Express + serverless-http)
│   └── db-pool.ts       # Postgres connection pool
├── schema.sql           # Database schema
├── ui/                  # React app (Vite)
│   └── src/
│       └── App.tsx      # Main component
└── package.json
```

## Setup

### Install Dependencies

```bash
npm install
cd ui && npm install && cd ..
```

### Build UI

```bash
npm run build
```

### Package for Lambda

```bash
npm run package
```

Creates `function.zip` ready for deployment.

## Deployment

### AWS Lambda Console

1. Create function: Runtime Node.js 20.x
2. Upload `function.zip`
3. Create Function URL (Auth: NONE, CORS: Enabled)
4. Set environment variables (see [ENV_SETUP_GUIDE.md](./docs/ENV_SETUP_GUIDE.md))

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `DB_HOST` | Postgres endpoint | Yes |
| `DB_PORT` | `5432` | No |
| `DB_NAME` | `embucket-poc-db` | No |
| `DB_USER` | `embucket` | No |
| `DB_PASSWORD` | Database password | Yes |
| `DB_SSL` | `true` for RDS | No |

## Database Setup

1. Create RDS Postgres database
2. Run `schema.sql` to create `query_history` table
3. Configure Lambda environment variables

See [ENV_SETUP_GUIDE.md](./docs/ENV_SETUP_GUIDE.md) for detailed steps.

## API Endpoints

### GET /api/data

Fetches query history from Postgres.

**Response:**
```json
{
  "query_history": [
    {
      "id": 1,
      "sql": "SELECT * FROM sales",
      "status": "completed",
      "duration_ms": 150,
      "created_at": "2024-01-01T00:00:00Z"
    }
  ]
}
```

### POST /api/data

Inserts a new query into history.

**Request:**
```json
{
  "table": "query_history",
  "data": {
    "sql": "SELECT * FROM users",
    "status": "completed",
    "duration_ms": 85
  }
}
```

**Response:**
```json
{
  "success": true,
  "id": 1
}
```

## Tech Stack

- **Backend:** Express, serverless-http, pg (PostgreSQL)
- **Frontend:** React, Vite, TypeScript, Tailwind CSS
- **AWS:** Lambda, Aurora RDS, Function URL

## Notes

- Uses `serverless-http` to wrap Express for Lambda
- Postgres pool uses `max: 1` (connections don't persist between invocations)
- Static files served from `ui/dist/`
- All routes except `/api/*` serve React app (SPA routing)
