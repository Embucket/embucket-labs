import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { cn } from '@/lib/utils';
import type { QueryRecord } from '@/orval/models';

interface SqlEditorRightPanelQueryItemStatusProps {
  status: QueryRecord['status'];
  error: QueryRecord['error'];
}

function SqlEditorRightPanelQueryItemStatus({
  status,
  error,
}: SqlEditorRightPanelQueryItemStatusProps) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span
          className={cn(
            'size-1.5 flex-shrink-0 rounded-full p-1',
            status === 'successful' ? 'bg-green-500' : 'bg-red-500',
          )}
        />
      </TooltipTrigger>
      {status === 'failed' && error && (
        <TooltipContent sideOffset={16} className="mr-6 max-w-[480px]">
          <span>{error}</span>
        </TooltipContent>
      )}
    </Tooltip>
  );
}

interface SqlEditorRightPanelQueryItemProps {
  status: QueryRecord['status'];
  error: QueryRecord['error'];
  query: string;
  time: string;
}

export function SqlEditorRightPanelQueryItem({
  status,
  error,
  query,
  time,
}: SqlEditorRightPanelQueryItemProps) {
  return (
    <li className="group flex w-full items-center justify-between text-nowrap">
      <div className="flex items-center overflow-hidden">
        <SqlEditorRightPanelQueryItemStatus status={status} error={error} />
        <span className="mx-2 truncate text-sm">{query}</span>
      </div>

      <span className="text-muted-foreground flex-shrink-0 text-xs">{time}</span>
    </li>
  );
}
