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

// Volume
const MEMORY_VOLUME = {
  name: 'mymemoryvolume',
};

async function createMemoryVolume(name) {
  console.log(`Memory Volume: ${name}`);
  try {
    const payload = { name, type: 'memory' };
    await apiClient.post('/ui/volumes', payload);
    console.log(`Memory Volume '${name}' created successfully.`);
  } catch (error) {
    console.error(`Failed creating memory volume '${name}'.`);
    throw error;
  }
}

// Database
const DATABASE = { name: 'mydb', volumeName: MEMORY_VOLUME.name };

async function createDatabase(databaseName, volumeName) {
  console.log(`Database: ${databaseName} on Volume: ${volumeName}`);
  try {
    await apiClient.post('/ui/databases', { volume: volumeName, name: databaseName });
    console.log(`Database '${databaseName}' created successfully.`);
  } catch (error) {
    console.error(`Failed creating database '${databaseName}'.`);
    throw error;
  }
}

// Schema
const SCHEMA = { databaseName: DATABASE.name, name: 'myschema' };

async function createSchema(databaseName, schemaName) {
  console.log(`Schema: ${databaseName}.${schemaName}`);
  try {
    await apiClient.post(`/ui/databases/${databaseName}/schemas`, { name: schemaName });
    console.log(`Schema '${databaseName}.${schemaName}' created successfully.`);
  } catch (error) {
    console.error(`Failed creating schema '${databaseName}.${schemaName}'.`);
    throw error;
  }
}

// Table
const TABLE_NAME = 'mytable';
const TABLE = {
  name: TABLE_NAME,
  databaseName: DATABASE.name,
  schemaName: SCHEMA.name,
  createQuery: `CREATE TABLE ${DATABASE.name}.${SCHEMA.name}.${TABLE_NAME} (id INT PRIMARY KEY, name VARCHAR(255));`,
  insertQuery: `INSERT INTO ${DATABASE.name}.${SCHEMA.name}.${TABLE_NAME} (id, name) VALUES (1, 'John Doe'), (2, 'Jane Smith');`,
};

async function createTable(databaseName, schemaName, tableName) {
  const fullName = `${databaseName}.${schemaName}.${tableName}`;
  console.log(`Table: ${fullName}`);
  try {
    await apiClient.post('/ui/queries', {
      query: `CREATE TABLE ${fullName} (id INT PRIMARY KEY, name VARCHAR(255));`,
    });
    await apiClient.post('/ui/queries', {
      query: `INSERT INTO ${fullName} (id, name) VALUES (1, 'John Doe'), (2, 'Jane Smith');`,
    });
    console.log(`Table '${fullName}' created.`);
  } catch (error) {
    console.error(`Failed creating table '${fullName}'.`);
    throw error;
  }
}

// Worksheet
const WORKSHEET = {
  name: 'myworksheet',
  content: `SELECT * FROM ${DATABASE.name}.${SCHEMA.name}.${TABLE.name};`,
};

async function createWorksheet(worksheetName, content) {
  console.log(`Worksheet: ${worksheetName}`);
  try {
    await apiClient.post('/ui/worksheets', {
      name: worksheetName,
      content,
    });
    console.log(`Worksheet '${worksheetName}' created.`);
  } catch (error) {
    console.error(`Failed creating worksheet '${worksheetName}'.`);
    throw error;
  }
}

(async function () {
  console.log(`🚀 Starting Resource Orchestration (API: ${API_BASE_URL})`);
  try {
    await createMemoryVolume(MEMORY_VOLUME.name);
    await createDatabase(DATABASE.name, DATABASE.volumeName);
    await createSchema(SCHEMA.databaseName, SCHEMA.name);
    await createTable(TABLE.databaseName, TABLE.schemaName, TABLE.name);
    await createWorksheet(WORKSHEET.name, WORKSHEET.content);
    console.log(`\n🎉 Orchestration script completed successfully.`);
  } catch (error) {
    console.error(`\n❌ Script execution failed.`);
    throw error;
  }
})();
