import { Search } from 'lucide-react';

import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { RefreshButton } from '@/components/ui/refresh-button';
import type { Schema } from '@/orval/models';

interface SchemasPageToolbarProps {
  search: string;
  onSetSearch: (search: string) => void;
  schemas: Schema[];
  isFetchingSchemas: boolean;
  onRefetchSchemas: () => Promise<unknown>;
}

export function SchemasPageToolbar({
  search,
  onSetSearch,
  schemas,
  isFetchingSchemas,
  onRefetchSchemas,
}: SchemasPageToolbarProps) {
  return (
    <div className="flex items-center justify-between gap-4 p-4">
      <p className="text-muted-foreground text-sm text-nowrap">
        {schemas.length ? `${schemas.length} schemas found` : ''}
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
        <RefreshButton isDisabled={isFetchingSchemas} onRefresh={onRefetchSchemas} />
      </div>
    </div>
  );
}
