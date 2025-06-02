import type { QueryRecord } from '@/orval/models';

interface QueryDetailsProps {
  queryRecord: QueryRecord;
}

export function QueryDetails({ queryRecord }: QueryDetailsProps) {
  return (
    <div className="grid grid-cols-1 gap-6 rounded-lg border p-4 md:grid-cols-2">
      <div>
        <div className="text-muted-foreground mb-1 text-xs">Status</div>
        <div className="font-medium">Success</div>
      </div>
      <div>
        <div className="text-muted-foreground mb-1 text-xs">Query ID</div>
        <div className="font-mono text-sm">{queryRecord.id}</div>
      </div>
      <div>
        <div className="text-muted-foreground mb-1 text-xs">Started At</div>
        <div className="font-mono text-sm">{queryRecord.startedAt}</div>
      </div>
      <div>
        <div className="text-muted-foreground mb-1 text-xs">Duration</div>
        <div className="font-mono text-sm">{(queryRecord.durationMs / 1000).toFixed(3) + 's'}</div>
      </div>
      <div>
        <div className="text-muted-foreground mb-1 text-xs">Database</div>
        <div className="font-mono text-sm">{queryRecord.databaseName}</div>
      </div>
      <div>
        <div className="text-muted-foreground mb-1 text-xs">User</div>
        <div className="font-mono text-sm">{queryRecord.userName}</div>
      </div>
    </div>
  );
}
