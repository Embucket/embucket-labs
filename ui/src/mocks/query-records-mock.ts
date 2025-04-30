import type { QueryRecord } from '@/orval/models';

export const QUERY_RECORDS_MOCK: QueryRecord[] = [
  {
    id: 1,
    query: 'SELECT * FROM users',
    error: '',
    result: {
      columns: [
        { name: 'id', type: 'INT' },
        { name: 'name', type: 'VARCHAR(255)' },
      ],
      rows: [
        [1, 'John'],
        [2, 'Charlie'],
        [3, 'Bob'],
      ],
    },
    resultCount: 0,
    startTime: '11:55:00 AM',
    endTime: '2023-10-01T12:00:05Z',
    durationMs: 5000,
    status: 'successful',
    worksheetId: 1,
  },
  {
    id: 2,
    query: 'SELECT * FROM orders',
    error: '',
    result: {
      columns: [],
      rows: [],
    },
    resultCount: 0,
    startTime: '12:00:05 AM',
    endTime: '2023-10-01T12:05:10Z',
    durationMs: 10000,
    status: 'failed',
    worksheetId: 1,
  },
];
