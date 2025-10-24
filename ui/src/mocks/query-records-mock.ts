import type { QueryRecord } from '@/orval/models';

export const QUERY_RECORDS_MOCK: QueryRecord[] = [
  {
    id: 8251111965593,
    query: 'SELECT * FROM information_schema.navigation_tree',
    startTime: '2025-06-02T18:13:54.406788Z',
    endTime: '2025-06-02T18:13:54.416249Z',
    durationMs: 9,
    resultCount: 18,
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
    status: 'failed',
    error:
      "Query execution error: DataFusion error: Error during planning: table 'embucket.public.dqwdqw' not found",
  },
];
