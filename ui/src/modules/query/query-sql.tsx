import type { QueryRecord } from '@/orval/models';

import { SQLEditor } from '../sql-editor/sql-editor';

interface QuerySQLProps {
  queryRecord: QueryRecord;
}

export function QuerySQL({ queryRecord }: QuerySQLProps) {
  return (
    <div className="grid grid-cols-1 gap-6 rounded-lg border p-4 md:grid-cols-2">
      <SQLEditor readonly content={queryRecord.query} />
    </div>
  );
}
