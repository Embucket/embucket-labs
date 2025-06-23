import { Search } from 'lucide-react';

import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { RefreshButton } from '@/components/ui/refresh-button';
import type { QueryRecord } from '@/orval/models';

interface QueriesHistoryPageToolbarProps {
  queries: QueryRecord[];
  isFetchingQueries: boolean;
  onRefetchQueries: () => Promise<unknown>;
}

export function QueriesHistoryPageToolbar({
  queries,
  isFetchingQueries,
  onRefetchQueries,
}: QueriesHistoryPageToolbarProps) {
  return (
    <div className="flex items-center justify-between gap-4 p-4">
      <p className="text-muted-foreground text-sm text-nowrap">
        {queries.length ? `${queries.length} queries found` : ''}
      </p>
      <div className="justify flex items-center justify-between gap-2">
        <InputRoot className="w-full">
          <InputIcon>
            <Search />
          </InputIcon>
          <Input disabled placeholder="Search" />
        </InputRoot>
        <RefreshButton isDisabled={isFetchingQueries} onRefresh={onRefetchQueries} />
      </div>
    </div>
  );
}
