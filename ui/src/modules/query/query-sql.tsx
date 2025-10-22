import { Skeleton } from '@/components/ui/skeleton';
import type { QueryRecord } from '@/orval/models';

import { Editor } from '../editor/editor';

interface QuerySQLProps {
  queryRecord?: QueryRecord;
}

export function QuerySQL({ queryRecord }: QuerySQLProps) {
  if (!queryRecord) {
    return (
      <div className="mb-4 rounded-lg border p-4">
        <div className="flex gap-2">
          <Skeleton className="h-5 w-10" />
          <Skeleton className="h-5 w-5" />
          <Skeleton className="h-5 w-10" />
          <Skeleton className="h-5 w-40" />
          <Skeleton className="h-5 w-20" />
          <Skeleton className="h-5 w-10" />
        </div>
      </div>
    );
  }
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
