/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable no-console */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
/* eslint-disable @typescript-eslint/restrict-template-expressions */
/* eslint-disable no-undef */
import axios from 'axios';

const API_BASE_URL = 'http://localhost:3000';

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: { 'Content-Type': 'application/json' },
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
];

async function createDatabases(databases) {
  for (const database of databases) {
    console.log(`Database: ${database.name} on Volume: ${database.volumeName}`);
    try {
      await apiClient.post('/ui/databases', { volume: database.volumeName, name: database.name });
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
    name: 'worksheet1',
    content: `SELECT * FROM ${TABLES[0].databaseName}.${TABLES[0].schemaName}.${TABLES[0].name};`,
  },
  {
    name: 'worksheet2',
    content: `SELECT COUNT(*) FROM ${TABLES[1].databaseName}.${TABLES[1].schemaName}.${TABLES[1].name};`,
  },
  {
    name: 'worksheet3',
    content: `SELECT * FROM ${TABLES[0].databaseName}.${TABLES[0].schemaName}.${TABLES[0].name} WHERE id = 1;`,
  },
  {
    name: 'worksheet4',
    content: `SELECT * FROM ${TABLES[1].databaseName}.${TABLES[1].schemaName}.${TABLES[1].name} WHERE id = 3;`,
  },
  {
    name: 'worksheet5',
    content: `SELECT * FROM ${TABLES[0].databaseName}.${TABLES[0].schemaName}.${TABLES[0].name} WHERE name LIKE 'John%';`,
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
