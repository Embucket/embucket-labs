import { useParams } from '@tanstack/react-router';
import { Search } from 'lucide-react';

import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { RefreshButton } from '@/components/ui/refresh-button';
import type { TablePreviewDataColumn } from '@/orval/models';
import { useGetTablePreviewData } from '@/orval/tables';

interface ColumnsPagePreviewDataToolbarProps {
  previewData: TablePreviewDataColumn[];
  isFetchingPreviewData: boolean;
  search: string;
  onSearch: (value: string) => void;
}

export function ColumnsPagePreviewDataToolbar({
  previewData,
  search,
  onSearch,
}: ColumnsPagePreviewDataToolbarProps) {
  const { databaseName, schemaName, tableName } = useParams({
    from: '/databases/$databaseName/schemas/$schemaName/tables/$tableName/columns/',
  });
  const { refetch: refetchPreviewData, isFetching: isFetchingPreviewData } = useGetTablePreviewData(
    databaseName,
    schemaName,
    tableName,
  );

  return (
    <div className="flex items-center justify-between gap-4 p-4">
      <p className="text-muted-foreground text-sm text-nowrap">
        {previewData[0]?.rows.length ? `${previewData[0]?.rows.length} rows found` : ''}
      </p>
      <div className="justify flex items-center justify-between gap-2">
        <InputRoot className="w-full">
          <InputIcon>
            <Search />
          </InputIcon>
          <Input
            value={search}
            onChange={(e) => onSearch(e.target.value)}
            placeholder="Search data"
          />
        </InputRoot>
        <RefreshButton isDisabled={isFetchingPreviewData} onRefresh={refetchPreviewData} />
      </div>
    </div>
  );
}
