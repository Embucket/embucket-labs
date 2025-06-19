import { Search } from 'lucide-react';

import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { RefreshButton } from '@/components/ui/refresh-button';
import type { Database } from '@/orval/models';

interface DatabasesPageToolbarProps {
  search: string;
  onSetSearch: (search: string) => void;
  databases: Database[];
  isFetchingDatabases: boolean;
  onRefetchDatabases: () => Promise<unknown>;
}

export function DatabasesPageToolbar({
  search,
  onSetSearch,
  databases,
  isFetchingDatabases,
  onRefetchDatabases,
}: DatabasesPageToolbarProps) {
  return (
    <div className="flex items-center justify-between gap-4 p-4">
      <p className="text-muted-foreground text-sm text-nowrap">
        {databases.length ? `${databases.length} databases found` : ''}
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
        <RefreshButton isDisabled={isFetchingDatabases} onRefresh={onRefetchDatabases} />
      </div>
    </div>
  );
}
