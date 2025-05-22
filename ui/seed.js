/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable no-console */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */

/* eslint-disable no-undef */
import axios from 'axios';

const API_BASE_URL = 'http://localhost:3000';
const ACCESS_TOKEN =
  'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJlbWJ1Y2tldCIsImF1ZCI6IjEyNy4wLjAuMSIsImlhdCI6MTc0Nzg0NjU0OCwiZXhwIjoxNzQ3ODQ3NDQ4fQ.LLdmvp4wb55tO2ePzUFDfzAmtfzANQWJFKks4Fqq4q8';

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${ACCESS_TOKEN}` },
});

// Volumes
const MEMORY_VOLUMES = [{ name: 'mymemoryvolume1' }, { name: 'mymemoryvolume2' }];

async function createMemoryVolumes(volumes) {
  for (const volume of volumes) {
    console.log(`Memory Volume: ${volume.name}`);
    try {
      const payload = { name: volume.name, type: 'memory' };
      await apiClient.post('/ui/volumes', payload);
      console.log(`Memory Volume '${volume.name}' created successfully.`);
    } catch (error) {
      console.error(`Failed creating memory volume '${volume.name}'.`);
      throw error;
    }
  }
}

// Databases
const DATABASES = [
  { name: 'mydb1', volumeName: MEMORY_VOLUMES[0].name },
  { name: 'mydb2', volumeName: MEMORY_VOLUMES[1].name },
  { name: 'mydb3', volumeName: MEMORY_VOLUMES[0].name },
  { name: 'mydb4', volumeName: MEMORY_VOLUMES[1].name },
  { name: 'mydb5', volumeName: MEMORY_VOLUMES[0].name },
  { name: 'mydb6', volumeName: MEMORY_VOLUMES[1].name },
  { name: 'mydb7', volumeName: MEMORY_VOLUMES[0].name },
  { name: 'mydb8', volumeName: MEMORY_VOLUMES[1].name },
  { name: 'mydb9', volumeName: MEMORY_VOLUMES[0].name },
  { name: 'mydb10', volumeName: MEMORY_VOLUMES[1].name },
];

async function createDatabases(databases) {
  for (const database of databases) {
    console.log(`Database: ${database.name} on Volume: ${database.volumeName}`);
    try {
      await apiClient.post('/ui/databases', {
        volume: database.volumeName,
        name: database.name,
        created_at: '',
        updated_at: '',
      });
      console.log(`Database '${database.name}' created successfully.`);
    } catch (error) {
      console.error(`Failed creating database '${database.name}'.`);
      throw error;
    }
  }
}

// Schemas
const SCHEMAS = [
  { databaseName: DATABASES[0].name, name: 'myschema1' },
  { databaseName: DATABASES[1].name, name: 'myschema2' },
  { databaseName: DATABASES[2].name, name: 'myschema3' },
  { databaseName: DATABASES[3].name, name: 'myschema4' },
  { databaseName: DATABASES[4].name, name: 'myschema5' },
  { databaseName: DATABASES[5].name, name: 'myschema6' },
  { databaseName: DATABASES[6].name, name: 'myschema7' },
  { databaseName: DATABASES[7].name, name: 'myschema8' },
  { databaseName: DATABASES[8].name, name: 'myschema9' },
  { databaseName: DATABASES[9].name, name: 'myschema10' },
];

async function createSchemas(schemas) {
  for (const schema of schemas) {
    console.log(`Schema: ${schema.databaseName}.${schema.name}`);
    try {
      await apiClient.post(`/ui/databases/${schema.databaseName}/schemas`, { name: schema.name });
      console.log(`Schema '${schema.databaseName}.${schema.name}' created successfully.`);
    } catch (error) {
      console.error(`Failed creating schema '${schema.databaseName}.${schema.name}'.`);
      throw error;
    }
  }
}

// Tables
const TABLES = [
  {
    name: 'mytable1',
    databaseName: SCHEMAS[0].databaseName,
    schemaName: SCHEMAS[0].name,
    createQuery: `CREATE TABLE ${SCHEMAS[0].databaseName}.${SCHEMAS[0].name}.mytable1 (id INT PRIMARY KEY, name VARCHAR(255));`,
    insertQuery: `INSERT INTO ${SCHEMAS[0].databaseName}.${SCHEMAS[0].name}.mytable1 (id, name) VALUES (1, 'John Doe'), (2, 'Jane Smith');`,
  },
  {
    name: 'mytable2',
    databaseName: SCHEMAS[1].databaseName,
    schemaName: SCHEMAS[1].name,
    createQuery: `CREATE TABLE ${SCHEMAS[1].databaseName}.${SCHEMAS[1].name}.mytable2 (id INT PRIMARY KEY, name VARCHAR(255));`,
    insertQuery: `INSERT INTO ${SCHEMAS[1].databaseName}.${SCHEMAS[1].name}.mytable2 (id, name) VALUES (3, 'Alice'), (4, 'Bob');`,
  },
  {
    name: 'mytable3',
    databaseName: SCHEMAS[2].databaseName,
    schemaName: SCHEMAS[2].name,
    createQuery: `CREATE TABLE ${SCHEMAS[2].databaseName}.${SCHEMAS[2].name}.mytable3 (id INT PRIMARY KEY, name VARCHAR(255));`,
    insertQuery: `INSERT INTO ${SCHEMAS[2].databaseName}.${SCHEMAS[2].name}.mytable3 (id, name) VALUES (5, 'Charlie'), (6, 'David');`,
  },
  {
    name: 'mytable4',
    databaseName: SCHEMAS[3].databaseName,
    schemaName: SCHEMAS[3].name,
    createQuery: `CREATE TABLE ${SCHEMAS[3].databaseName}.${SCHEMAS[3].name}.mytable4 (id INT PRIMARY KEY, name VARCHAR(255));`,
    insertQuery: `INSERT INTO ${SCHEMAS[3].databaseName}.${SCHEMAS[3].name}.mytable4 (id, name) VALUES (7, 'Eve'), (8, 'Frank');`,
  },
  {
    name: 'mytable5',
    databaseName: SCHEMAS[4].databaseName,
    schemaName: SCHEMAS[4].name,
    createQuery: `CREATE TABLE ${SCHEMAS[4].databaseName}.${SCHEMAS[4].name}.mytable5 (id INT PRIMARY KEY, name VARCHAR(255));`,
    insertQuery: `INSERT INTO ${SCHEMAS[4].databaseName}.${SCHEMAS[4].name}.mytable5 (id, name) VALUES (9, 'Grace'), (10, 'Heidi');`,
  },
  {
    name: 'mytable6',
    databaseName: SCHEMAS[5].databaseName,
    schemaName: SCHEMAS[5].name,
    createQuery: `CREATE TABLE ${SCHEMAS[5].databaseName}.${SCHEMAS[5].name}.mytable6 (id INT PRIMARY KEY, name VARCHAR(255));`,
    insertQuery: `INSERT INTO ${SCHEMAS[5].databaseName}.${SCHEMAS[5].name}.mytable6 (id, name) VALUES (11, 'Ivan'), (12, 'Judy');`,
  },
  {
    name: 'mytable7',
    databaseName: SCHEMAS[6].databaseName,
    schemaName: SCHEMAS[6].name,
    createQuery: `CREATE TABLE ${SCHEMAS[6].databaseName}.${SCHEMAS[6].name}.mytable7 (id INT PRIMARY KEY, name VARCHAR(255));`,
    insertQuery: `INSERT INTO ${SCHEMAS[6].databaseName}.${SCHEMAS[6].name}.mytable7 (id, name) VALUES (13, 'Karl'), (14, 'Leo');`,
  },
  {
    name: 'mytable8',
    databaseName: SCHEMAS[7].databaseName,
    schemaName: SCHEMAS[7].name,
    createQuery: `CREATE TABLE ${SCHEMAS[7].databaseName}.${SCHEMAS[7].name}.mytable8 (id INT PRIMARY KEY, name VARCHAR(255));`,
    insertQuery: `INSERT INTO ${SCHEMAS[7].databaseName}.${SCHEMAS[7].name}.mytable8 (id, name) VALUES (15, 'Mallory'), (16, 'Nina');`,
  },
  {
    name: 'mytable9',
    databaseName: SCHEMAS[8].databaseName,
    schemaName: SCHEMAS[8].name,
    createQuery: `CREATE TABLE ${SCHEMAS[8].databaseName}.${SCHEMAS[8].name}.mytable9 (id INT PRIMARY KEY, name VARCHAR(255));`,
    insertQuery: `INSERT INTO ${SCHEMAS[8].databaseName}.${SCHEMAS[8].name}.mytable9 (id, name) VALUES (17, 'Oscar'), (18, 'Peggy');`,
  },
  {
    name: 'mytable10',
    databaseName: SCHEMAS[9].databaseName,
    schemaName: SCHEMAS[9].name,
    createQuery: `CREATE TABLE ${SCHEMAS[9].databaseName}.${SCHEMAS[9].name}.mytable10 (id INT PRIMARY KEY, name VARCHAR(255));`,
    insertQuery: `INSERT INTO ${SCHEMAS[9].databaseName}.${SCHEMAS[9].name}.mytable10 (id, name) VALUES (19, 'Quentin'), (20, 'Rupert');`,
  },
];

async function createTables(tables) {
  for (const table of tables) {
    const fullName = `${table.databaseName}.${table.schemaName}.${table.name}`;
    console.log(`Table: ${fullName}`);
    try {
      await apiClient.post('/ui/queries', { query: table.createQuery });
      await apiClient.post('/ui/queries', { query: table.insertQuery });
      console.log(`Table '${fullName}' created.`);
    } catch (error) {
      console.error(`Failed creating table '${fullName}'.`);
      throw error;
    }
  }
}

// Worksheets
const WORKSHEETS = [
  {
    name: 'myworksheet1',
    content: `SELECT * FROM ${TABLES[0].databaseName}.${TABLES[0].schemaName}.${TABLES[0].name};`,
  },
  {
    name: 'myworksheet2',
    content: `SELECT COUNT(*) FROM ${TABLES[1].databaseName}.${TABLES[1].schemaName}.${TABLES[1].name};`,
  },
  {
    name: 'myworksheet3',
    content: `SELECT * FROM ${TABLES[2].databaseName}.${TABLES[2].schemaName}.${TABLES[2].name} WHERE id = 5;`,
  },
  {
    name: 'myworksheet4',
    content: `SELECT * FROM ${TABLES[3].databaseName}.${TABLES[3].schemaName}.${TABLES[3].name} WHERE name LIKE 'E%';`,
  },
  {
    name: 'myworksheet5',
    content: `SELECT * FROM ${TABLES[4].databaseName}.${TABLES[4].schemaName}.${TABLES[4].name} ORDER BY name;`,
  },
  {
    name: 'myworksheet6',
    content: `SELECT * FROM ${TABLES[5].databaseName}.${TABLES[5].schemaName}.${TABLES[5].name} WHERE id BETWEEN 11 AND 12;`,
  },
  {
    name: 'myworksheet7',
    content: `SELECT * FROM ${TABLES[6].databaseName}.${TABLES[6].schemaName}.${TABLES[6].name};`,
  },
  {
    name: 'myworksheet8',
    content: `SELECT COUNT(*) FROM ${TABLES[7].databaseName}.${TABLES[7].schemaName}.${TABLES[7].name};`,
  },
  {
    name: 'myworksheet9',
    content: `SELECT * FROM ${TABLES[8].databaseName}.${TABLES[8].schemaName}.${TABLES[8].name} WHERE id = 17;`,
  },
  {
    name: 'myworksheet10',
    content: `SELECT * FROM ${TABLES[9].databaseName}.${TABLES[9].schemaName}.${TABLES[9].name} WHERE name LIKE 'Q%';`,
  },
];

async function createWorksheets(worksheets) {
  for (const worksheet of worksheets) {
    console.log(`Worksheet: ${worksheet.name}`);
    try {
      await apiClient.post('/ui/worksheets', {
        name: worksheet.name,
        content: worksheet.content,
      });
      console.log(`Worksheet '${worksheet.name}' created.`);
    } catch (error) {
      console.error(`Failed creating worksheet '${worksheet.name}'.`);
      throw error;
    }
  }
}

(async function () {
  console.log(`🚀 Starting Resource Orchestration (API: ${API_BASE_URL})`);
  try {
    await createMemoryVolumes(MEMORY_VOLUMES);
    await createDatabases(DATABASES);
    await createSchemas(SCHEMAS);
    await createTables(TABLES);
    await createWorksheets(WORKSHEETS);
    console.log(`\n🎉 Orchestration script completed successfully.`);
  } catch (error) {
    console.error(`\n❌ Script execution failed.`);
    throw error;
  }
})();
