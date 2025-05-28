import { useParams } from '@tanstack/react-router';
import { Search } from 'lucide-react';

import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { RefreshButton } from '@/components/ui/refresh-button';
import type { Table } from '@/orval/models';
import { useGetTables } from '@/orval/tables';

interface TablesPageToolbarProps {
  tables: Table[];
  isFetchingTables: boolean;
}

export function TablesPageToolbar({ tables }: TablesPageToolbarProps) {
  const { databaseName, schemaName } = useParams({
    from: '/databases/$databaseName/schemas/$schemaName/tables/',
  });

  const { refetch: refetchTables, isFetching: isFetchingTables } = useGetTables(
    databaseName,
    schemaName,
  );

  return (
    <div className="flex items-center justify-between gap-4 p-4">
      <p className="text-muted-foreground text-sm text-nowrap">{tables.length} tables found</p>
      <div className="justify flex items-center justify-between gap-2">
        <InputRoot className="w-full">
          <InputIcon>
            <Search />
          </InputIcon>
          <Input disabled placeholder="Search" />
        </InputRoot>
        <RefreshButton isDisabled={isFetchingTables} onRefresh={refetchTables} />
      </div>
    </div>
  );
}
