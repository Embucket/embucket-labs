import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { Skeleton } from '@/components/ui/skeleton';
import { formatTime } from '@/lib/formatTime';
import { cn } from '@/lib/utils';
import type { QueryRecord } from '@/orval/models';

const SkeletonRow = () => {
  return <Skeleton className="size-5 w-[150px]" />;
};

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
  queryRecord?: QueryRecord;
}

// TODO: DRY Progress, Status
export function QueryDetails({ queryRecord }: QueryDetailsProps) {
  const status = queryRecord?.status;

  return (
    <div className="grid max-h-[132px] grid-cols-3 gap-4 rounded-lg border p-4">
      <DetailItem label="Query ID">{queryRecord ? queryRecord.id : <SkeletonRow />}</DetailItem>

      <DetailItem label="Status">
        {status ? (
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
        ) : (
          <Skeleton className="h-[22px] w-20" />
        )}
      </DetailItem>

      <DetailItem label="Duration">
        <div className="flex max-w-[<SkeletonRow/>0px] items-center gap-2">
          {queryRecord ? (
            <>
              <Progress value={100} />
              <span>{`${queryRecord.durationMs}ms`}</span>
            </>
          ) : (
            <Skeleton className="h-5 w-[200px]" />
          )}
        </div>
      </DetailItem>

      <DetailItem label="Start Time">
        {queryRecord ? formatTime(queryRecord.startTime) : <SkeletonRow />}
      </DetailItem>

      <DetailItem label="End Time">
        {queryRecord ? formatTime(queryRecord.endTime) : <SkeletonRow />}
      </DetailItem>

      <DetailItem label="Rows count">
        {queryRecord ? queryRecord.resultCount : <SkeletonRow />}
      </DetailItem>
    </div>
  );
}
