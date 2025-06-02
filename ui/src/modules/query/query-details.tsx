import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { formatTime } from '@/lib/formatTime';
import { cn } from '@/lib/utils';
import type { QueryRecord } from '@/orval/models';

interface DetailItemProps {
  label: string;
  children: React.ReactNode;
}

function DetailItem({ label, children }: DetailItemProps) {
  return (
    <div>
      <div className="text-muted-foreground mb-1 text-xs">{label}</div>
      <div className="font-mono text-sm">{children}</div>
    </div>
  );
}

// error: string;
interface QueryDetailsProps {
  queryRecord: QueryRecord;
}

// TODO: DRY Progress, Status
export function QueryDetails({ queryRecord }: QueryDetailsProps) {
  const status = queryRecord.status;

  return (
    <div className="grid grid-cols-1 gap-4 rounded-lg border p-4 md:grid-cols-2 lg:grid-cols-3">
      <DetailItem label="Query ID">{queryRecord.id}</DetailItem>

      <DetailItem label="Status">
        <div className="font-medium">
          <Badge variant="outline">
            <span
              className={cn(
                'capitalize',
                status === 'successful' && 'text-green-500',
                status === 'failed' && 'text-red-500',
              )}
            >
              {status}
            </span>
          </Badge>
        </div>
      </DetailItem>

      <DetailItem label="Duration">
        <div className="flex max-w-[120px] items-center gap-2">
          <Progress value={100} />
          <span>{queryRecord.durationMs}ms</span>
        </div>
      </DetailItem>

      <DetailItem label="Start Time">{formatTime(queryRecord.startTime)}</DetailItem>

      <DetailItem label="End Time">{formatTime(queryRecord.endTime)}</DetailItem>

      <DetailItem label="Rows count">{queryRecord.resultCount}</DetailItem>
    </div>
  );
}
