import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { cn } from '@/lib/utils';
import type { QueryRecord } from '@/orval/models';

interface EditorRightPanelQueryItemStatusProps {
  status: QueryRecord['status'];
  error: QueryRecord['error'];
}

function EditorRightPanelQueryItemStatus({ status, error }: EditorRightPanelQueryItemStatusProps) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span
          className={cn('size-1.5 shrink-0 rounded-full p-1', {
            'bg-yellow-500': status === 'running',
            'bg-green-500': status === 'successful',
            'bg-red-500': status !== 'running' && status !== 'successful',
          })}
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

interface EditorRightPanelQueryItemProps {
  status: QueryRecord['status'];
  error: QueryRecord['error'];
  query: string;
  time: string;
}

export function EditorRightPanelQueryItem({
  status,
  error,
  query,
  time,
}: EditorRightPanelQueryItemProps) {
  return (
    <li className="group flex w-full items-center justify-between text-nowrap">
      <div className="flex items-center overflow-hidden">
        <EditorRightPanelQueryItemStatus status={status} error={error} />
        <span className="mx-2 truncate text-sm">{query}</span>
      </div>

      <span className="text-muted-foreground shrink-0 text-xs">{time}</span>
    </li>
  );
}
