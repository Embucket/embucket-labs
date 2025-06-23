import { Search } from 'lucide-react';

import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { RefreshButton } from '@/components/ui/refresh-button';
import type { Column } from '@/orval/models';

interface ColumnsPageToolbarProps {
  columns: Column[];
  isFetchingColumns: boolean;
  search: string;
  onSearch: (value: string) => void;
  onRefetchColumns: () => Promise<unknown>;
}

export function ColumnsPageToolbar({
  columns,
  isFetchingColumns,
  search,
  onSearch,
  onRefetchColumns,
}: ColumnsPageToolbarProps) {
  return (
    <div className="flex items-center justify-between gap-4 p-4">
      <p className="text-muted-foreground text-sm text-nowrap">
        {columns.length ? `${columns.length} columns found` : ''}
      </p>
      <div className="justify flex items-center justify-between gap-2">
        <InputRoot className="w-full">
          <InputIcon>
            <Search />
          </InputIcon>
          <Input
            value={search}
            onChange={(e) => onSearch(e.target.value)}
            placeholder="Search columns"
          />
        </InputRoot>
        <RefreshButton isDisabled={isFetchingColumns} onRefresh={onRefetchColumns} />
      </div>
    </div>
  );
}
