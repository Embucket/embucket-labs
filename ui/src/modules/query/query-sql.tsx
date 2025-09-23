import type { QueryRecord } from '@/orval/models';

import { Editor } from '../editor/editor';

interface QuerySQLProps {
  queryRecord: QueryRecord;
}

export function QuerySQL({ queryRecord }: QuerySQLProps) {
  return (
    <div className="mb-4 gap-6 rounded-lg border p-4">
      {queryRecord.error ? (
        <span className="text-sm text-red-500">{queryRecord.error}</span>
      ) : (
        <Editor readonly content={queryRecord.query} />
      )}
    </div>
  );
}
