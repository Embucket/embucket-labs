import { Search } from 'lucide-react';

import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { RefreshButton } from '@/components/ui/refresh-button';
import { useGetDatabases } from '@/orval/databases';
import type { Database } from '@/orval/models';

interface DatabasesPageToolbarProps {
  databases: Database[];
  isFetchingDatabases: boolean;
}

export function DatabasesPageToolbar({ databases }: DatabasesPageToolbarProps) {
  const { refetch: refetchDatabases, isFetching: isFetchingDatabases } = useGetDatabases();

  return (
    <div className="flex items-center justify-between gap-4 p-4">
      <p className="text-muted-foreground text-sm text-nowrap">
        {databases.length} databases found
      </p>
      <div className="justify flex items-center justify-between gap-2">
        <InputRoot className="w-full">
          <InputIcon>
            <Search />
          </InputIcon>
          <Input disabled placeholder="Search" />
        </InputRoot>
        <RefreshButton isDisabled={isFetchingDatabases} onRefresh={refetchDatabases} />
      </div>
    </div>
  );
}
