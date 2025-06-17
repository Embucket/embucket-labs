import { Search } from 'lucide-react';

import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { RefreshButton } from '@/components/ui/refresh-button';
import type { Table } from '@/orval/models';

interface TablesPageToolbarProps {
  search: string;
  onSetSearch: (search: string) => void;
  tables: Table[];
  isFetchingTables: boolean;
  onRefetchTables: () => Promise<unknown>;
}

export function TablesPageToolbar({
  search,
  onSetSearch,
  tables,
  isFetchingTables,
  onRefetchTables,
}: TablesPageToolbarProps) {
  return (
    <div className="flex items-center justify-between gap-4 p-4">
      <p className="text-muted-foreground text-sm text-nowrap">
        {tables.length ? `${tables.length} tables found` : ''}
      </p>
      <div className="justify flex items-center justify-between gap-2">
        <InputRoot className="w-full">
          <InputIcon>
            <Search />
          </InputIcon>
          <Input
            value={search}
            onChange={(e) => onSetSearch(e.target.value)}
            placeholder="Search"
          />
        </InputRoot>
        <RefreshButton isDisabled={isFetchingTables} onRefresh={onRefetchTables} />
      </div>
    </div>
  );
}
