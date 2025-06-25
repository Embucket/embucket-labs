import { Search } from 'lucide-react';

import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { RefreshButton } from '@/components/ui/refresh-button';
import type { TablePreviewDataColumn } from '@/orval/models';

interface DataPreviewPageToolbarProps {
  previewData: TablePreviewDataColumn[];
  isFetchingPreviewData: boolean;
  search: string;
  onSearch: (value: string) => void;
  onRefetchPreviewData: () => Promise<unknown>;
}

export function DataPreviewPageToolbar({
  previewData,
  isFetchingPreviewData,
  search,
  onSearch,
  onRefetchPreviewData,
}: DataPreviewPageToolbarProps) {
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
        <RefreshButton isDisabled={isFetchingPreviewData} onRefresh={onRefetchPreviewData} />
      </div>
    </div>
  );
}
