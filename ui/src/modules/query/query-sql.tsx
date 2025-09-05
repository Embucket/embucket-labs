import type { QueryRecord } from '@/orval/models';

import { SQLEditor } from '../sql-editor/sql-editor';

interface QuerySQLProps {
  queryRecord: QueryRecord;
}

export function QuerySQL({ queryRecord }: QuerySQLProps) {
  return (
    <div className="mb-4 gap-6 rounded-lg border p-4">
      {queryRecord.error ? (
        <span className="text-sm text-red-500">{queryRecord.error}</span>
      ) : (
        <SQLEditor readonly content={queryRecord.query} />
      )}
    </div>
  );
}
