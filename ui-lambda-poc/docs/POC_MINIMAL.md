# Минимальный POC: LAMBADA UI

## Цель
Создать работающий прототип UI на Node.js Lambda, который может выполнять SQL запросы через существующий Rust Lambda.

## Шаги

### 1. Подготовка окружения
```bash
# Установить AWS CLI и настроить credentials
aws configure

# Установить Node.js 20+
node --version

# Установить зависимости (если нужно)
npm install -g serverless
# или
npm install -g @aws-amplify/cli
```

### 2. Создать Node.js Lambda функцию

**Структура проекта:**
```
lambda-ui/
├── index.js          # Lambda handler
├── package.json      # Зависимости
└── ui/               # Статические файлы UI (опционально)
    ├── index.html
    └── assets/
```

**package.json:**
```json
{
  "name": "embucket-ui-lambda",
  "version": "1.0.0",
  "type": "module",
  "dependencies": {
    "@aws-sdk/client-lambda": "^3.0.0",
    "pg": "^8.11.3"
  }
}
```

**index.js (минимальный handler):**
```javascript
export const handler = async (event) => {
  // Если запрос к UI - отдаем HTML
  if (event.path === '/' || event.path === '/ui') {
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'text/html' },
      body: '<html><body><h1>Embucket UI</h1><p>POC</p></body></html>'
    };
  }
  
  // Если запрос к API - проксируем в Rust Lambda
  if (event.path === '/api/query') {
    // TODO: вызвать Rust Lambda через AWS SDK
    return {
      statusCode: 200,
      body: JSON.stringify({ result: 'query executed' })
    };
  }
  
  return { statusCode: 404 };
};
```

### 3. Деплой Lambda через AWS Console

**Вариант A: Через AWS Console (быстрее)**
1. Открыть AWS Lambda Console
2. Create function → Author from scratch
3. Runtime: Node.js 20.x
4. Upload code (zip с index.js и package.json)
5. Create Function URL → Enable
6. Скопировать Function URL

**Вариант B: Через AWS CLI**
```bash
# Создать deployment package
cd lambda-ui
zip -r function.zip . -x "*.git*"

# Создать функцию
aws lambda create-function \
  --function-name embucket-ui-poc \
  --runtime nodejs20.x \
  --role arn:aws:iam::ACCOUNT:role/lambda-execution-role \
  --handler index.handler \
  --zip-file fileb://function.zip

# Создать Function URL
aws lambda create-function-url-config \
  --function-name embucket-ui-poc \
  --auth-type AWS_IAM
```

### 4. Создать Postgres базу данных

**Вариант A: RDS Postgres (проще для POC)**
```bash
# Создать RDS Postgres instance
aws rds create-db-instance \
  --db-instance-identifier embucket-poc-db \
  --db-instance-class db.t3.micro \
  --engine postgres \
  --engine-version 15.4 \
  --master-username embucket \
  --master-user-password YOUR_PASSWORD \
  --allocated-storage 20 \
  --publicly-accessible \
  --vpc-security-group-ids sg-xxxxx

# Дождаться создания (5-10 минут)
aws rds describe-db-instances \
  --db-instance-identifier embucket-poc-db \
  --query 'DBInstances[0].Endpoint.Address' \
  --output text
```

**Вариант B: Aurora Serverless v2 (рекомендуется для продакшена)**
```bash
# Создать Aurora Serverless v2 cluster
aws rds create-db-cluster \
  --db-cluster-identifier embucket-poc-cluster \
  --engine aurora-postgresql \
  --engine-version 15.4 \
  --master-username embucket \
  --master-user-password YOUR_PASSWORD \
  --serverless-v2-scaling-configuration MinCapacity=0.5,MaxCapacity=2
```

**Через AWS Console:**
1. RDS → Create database
2. Engine: PostgreSQL (или Aurora PostgreSQL)
3. Template: Free tier (для POC)
4. DB instance identifier: `embucket-poc-db`
5. Master username: `embucket`
6. Master password: (сохранить!)
7. Public access: Yes (для POC)
8. Create database

### 5. Настроить RDS Proxy (опционально, но рекомендуется)

**Зачем:** Переиспользование соединений между Lambda вызовами

```bash
# Создать RDS Proxy
aws rds create-db-proxy \
  --db-proxy-name embucket-proxy \
  --engine-family POSTGRESQL \
  --auth UsernamePassword \
  --role-arn arn:aws:iam::ACCOUNT:role/rds-proxy-role \
  --vpc-subnet-ids subnet-xxx subnet-yyy \
  --vpc-security-group-ids sg-xxx \
  --targets TargetGroupName=default,DBInstanceIdentifiers=embucket-poc-db

# Получить endpoint
aws rds describe-db-proxies \
  --db-proxy-name embucket-proxy \
  --query 'DBProxies[0].Endpoint' \
  --output text
```

**Для POC можно пропустить** и подключаться напрямую к RDS.

### 6. Создать схему базы данных

**Подключиться к Postgres:**
```bash
# Установить psql (если нет)
brew install postgresql  # macOS
# или
sudo apt-get install postgresql-client  # Linux

# Подключиться
psql -h YOUR_RDS_ENDPOINT -U embucket -d postgres
```

**Создать базовую схему:**
```sql
-- Создать базу данных для метаданных
CREATE DATABASE embucket_metadata;

\c embucket_metadata

-- Таблица для истории запросов
CREATE TABLE query_history (
  id SERIAL PRIMARY KEY,
  sql TEXT NOT NULL,
  status VARCHAR(50),
  duration_ms INTEGER,
  created_at TIMESTAMP DEFAULT NOW()
);

-- Таблица для метаданных таблиц (упрощенная)
CREATE TABLE tables_metadata (
  id SERIAL PRIMARY KEY,
  database_name VARCHAR(255),
  schema_name VARCHAR(255),
  table_name VARCHAR(255),
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(database_name, schema_name, table_name)
);

-- Вставить тестовые данные
INSERT INTO tables_metadata (database_name, schema_name, table_name)
VALUES ('embucket', 'public', 'sales');

-- Проверить
SELECT * FROM query_history;
SELECT * FROM tables_metadata;
```

### 7. Настроить IAM роль для доступа к RDS

**Обновить IAM политику Lambda:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction"
      ],
      "Resource": "arn:aws:lambda:*:*:function:embucket-*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "rds-db:connect"
      ],
      "Resource": "arn:aws:rds-db:REGION:ACCOUNT:dbuser:DB_INSTANCE_ID/embucket"
    }
  ]
}
```

**Для RDS Proxy добавить:**
```json
{
  "Effect": "Allow",
  "Action": [
    "rds-db:connect"
  ],
  "Resource": "arn:aws:rds-db:REGION:ACCOUNT:dbuser:PROXY_ID/embucket"
}
```

### 8. Подключить Postgres в Lambda

**Обновить index.js:**
```javascript
import pg from 'pg';
const { Pool } = pg;

// Создать connection pool
const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT || 5432,
  database: process.env.DB_NAME || 'embucket_metadata',
  user: process.env.DB_USER || 'embucket',
  password: process.env.DB_PASSWORD,
  ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : false,
  max: 1, // Lambda не держит соединения между вызовами
});

export const handler = async (event) => {
  // Если запрос к UI - отдаем HTML
  if (event.path === '/' || event.path === '/ui') {
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'text/html' },
      body: '<html><body><h1>Embucket UI</h1><p>POC работает!</p></body></html>'
    };
  }
  
  // API: Получить список таблиц из Postgres
  if (event.path === '/api/tables') {
    try {
      const result = await pool.query(
        'SELECT database_name, schema_name, table_name FROM tables_metadata'
      );
      return {
        statusCode: 200,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tables: result.rows })
      };
    } catch (error) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: error.message })
      };
    }
  }
  
  // API: Сохранить историю запроса
  if (event.path === '/api/queries/history' && event.httpMethod === 'POST') {
    try {
      const body = JSON.parse(event.body || '{}');
      const result = await pool.query(
        'INSERT INTO query_history (sql, status, duration_ms) VALUES ($1, $2, $3) RETURNING id',
        [body.sql, body.status || 'completed', body.duration_ms || 0]
      );
      return {
        statusCode: 200,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: result.rows[0].id })
      };
    } catch (error) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: error.message })
      };
    }
  }
  
  // Если запрос к API - проксируем в Rust Lambda
  if (event.path === '/api/query') {
    // TODO: вызвать Rust Lambda через AWS SDK
    return {
      statusCode: 200,
      body: JSON.stringify({ result: 'query executed' })
    };
  }
  
  return { statusCode: 404 };
};
```

**Добавить environment variables в Lambda:**
```bash
aws lambda update-function-configuration \
  --function-name embucket-ui-poc \
  --environment Variables="{
    DB_HOST=your-rds-endpoint.region.rds.amazonaws.com,
    DB_PORT=5432,
    DB_NAME=embucket_metadata,
    DB_USER=embucket,
    DB_PASSWORD=YOUR_PASSWORD,
    DB_SSL=true
  }"
```

**Или через Console:**
1. Lambda → Configuration → Environment variables
2. Добавить переменные:
   - `DB_HOST` = endpoint RDS
   - `DB_PORT` = 5432
   - `DB_NAME` = embucket_metadata
   - `DB_USER` = embucket
   - `DB_PASSWORD` = ваш пароль
   - `DB_SSL` = true

### 9. Протестировать

```bash
# Получить Function URL
FUNCTION_URL=$(aws lambda get-function-url-config \
  --function-name embucket-ui-poc \
  --query FunctionUrl --output text)

# Протестировать
curl $FUNCTION_URL
```

### 10. Интеграция с Rust Lambda (опционально)

**Вызов Rust Lambda из Node.js:**
```javascript
import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda';

const lambda = new LambdaClient({ region: 'us-east-1' });

export const handler = async (event) => {
  if (event.path === '/api/query') {
    const command = new InvokeCommand({
      FunctionName: 'embucket-rust-lambda',
      Payload: JSON.stringify({
        sql: event.queryStringParameters?.sql || 'SELECT 1'
      })
    });
    
    const response = await lambda.send(command);
    const result = JSON.parse(new TextDecoder().decode(response.Payload));
    
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(result)
    };
  }
  
  // ... остальной код
};
```

## Результат

После выполнения шагов у вас будет:
- ✅ Работающая Node.js Lambda функция
- ✅ Function URL для доступа
- ✅ Postgres база данных с базовой схемой
- ✅ Подключение Lambda к Postgres
- ✅ API endpoints для работы с метаданными
- ✅ Базовый UI endpoint
- ✅ Возможность расширения для вызова Rust Lambda

## Следующие шаги

1. Добавить простой HTML UI с формой для SQL запросов
2. Интегрировать с существующей Rust Lambda
3. Добавить обработку ошибок
4. Настроить CORS для работы с браузером
5. Настроить RDS Proxy для production (опционально)
6. Расширить схему БД (users, sessions, worksheets)

## Время выполнения

- Шаги 1-3: ~15 минут
- Шаги 4-6: ~10 минут (создание RDS)
- Шаг 7: ~5 минут (схема БД)
- Шаги 8-9: ~10 минут (интеграция)
- Шаг 10: ~10 минут (Rust Lambda)

**Итого: ~50 минут для POC с Postgres**

## Важные замечания

⚠️ **Безопасность:**
- Не храните пароли в коде, используйте AWS Secrets Manager
- Для production используйте RDS Proxy и VPC
- Настройте security groups правильно

⚠️ **Connection Pooling:**
- Lambda не держит соединения между вызовами
- Используйте `max: 1` в Pool для Lambda
- Для production используйте RDS Proxy

⚠️ **SSL:**
- RDS требует SSL соединения
- Установите `DB_SSL=true` в environment variables

