import type { QueryRecord } from '@/orval/models';

export const QUERY_RECORDS_MOCK: QueryRecord[] = [
  {
    id: 8251111965593,
    query: 'SELECT * FROM information_schema.navigation_tree',
    startTime: '2025-06-02T18:13:54.406788Z',
    endTime: '2025-06-02T18:13:54.416249Z',
    durationMs: 9,
    resultCount: 18,
    result: {
      columns: [
        {
          name: 'database',
          type: 'text',
        },
        {
          name: 'schema',
          type: 'text',
        },
        {
          name: 'table',
          type: 'text',
        },
        {
          name: 'table_type',
          type: 'text',
        },
      ],
      rows: [
        ['slatedb', null, null, null],
        ['slatedb', 'meta', null, null],
        ['slatedb', 'meta', 'schemas', 'VIEW'],
        ['slatedb', 'meta', 'databases', 'VIEW'],
        ['slatedb', 'meta', 'tables', 'VIEW'],
        ['slatedb', 'meta', 'volumes', 'VIEW'],
        ['slatedb', 'history', null, null],
        ['slatedb', 'history', 'worksheets', 'VIEW'],
        ['slatedb', 'history', 'queries', 'VIEW'],
        ['slatedb', 'information_schema', 'df_settings', 'VIEW'],
        ['slatedb', 'information_schema', 'columns', 'VIEW'],
      ],
    },
    status: 'successful',
    error: '',
  },
  {
    id: 8251112207655,
    query: 'SELECT * FROM slatedb.meta.volumes ORDER BY volume_name DESC LIMIT 250',
    startTime: '2025-06-02T18:09:52.344996Z',
    endTime: '2025-06-02T18:09:52.360947Z',
    durationMs: 15,
    resultCount: 0,
    result: {
      columns: [
        {
          name: 'volume_name',
          type: 'text',
        },
        {
          name: 'volume_type',
          type: 'text',
        },
        {
          name: 'created_at',
          type: 'text',
        },
        {
          name: 'updated_at',
          type: 'text',
        },
      ],
      rows: [],
    },
    status: 'failed',
    error:
      "Query execution error: DataFusion error: Error during planning: table 'embucket.public.dqwdqw' not found",
  },
];
