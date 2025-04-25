import { cn } from '@/lib/utils';
import type { QueryRecord } from '@/orval/models';

interface SqlEditorRightPanelQueryItemProps {
  status: QueryRecord['status'];
  query: string;
  time: string;
}

export function SqlEditorRightPanelQueryItem({
  status,
  query,
  time,
}: SqlEditorRightPanelQueryItemProps) {
  return (
    <li className="group flex w-full items-center justify-between text-nowrap">
      <div className="flex items-center overflow-hidden">
        <span
          className={cn(
            'size-1.5 flex-shrink-0 rounded-full',
            status === 'successful' ? 'bg-green-500' : 'bg-red-500',
          )}
        />
        <span className="mx-2 truncate text-sm">{query}</span>
      </div>

      <span className="text-muted-foreground flex-shrink-0 text-xs">{time}</span>
    </li>
  );
}
