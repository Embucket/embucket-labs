import { useParams } from '@tanstack/react-router';
import { Search } from 'lucide-react';

import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { RefreshButton } from '@/components/ui/refresh-button';
import type { Column } from '@/orval/models';
import { useGetTableColumns } from '@/orval/tables';

interface ColumnsPageToolbarProps {
  columns: Column[];
  isFetchingColumns: boolean;
  search: string;
  onSearch: (value: string) => void;
}

export function ColumnsPageToolbar({ columns, search, onSearch }: ColumnsPageToolbarProps) {
  const { databaseName, schemaName, tableName } = useParams({
    from: '/databases/$databaseName/schemas/$schemaName/tables/$tableName/columns/',
  });
  const { refetch: refetchColumns, isFetching: isFetchingColumns } = useGetTableColumns(
    databaseName,
    schemaName,
    tableName,
  );

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
        <RefreshButton isDisabled={isFetchingColumns} onRefresh={refetchColumns} />
      </div>
    </div>
  );
}
