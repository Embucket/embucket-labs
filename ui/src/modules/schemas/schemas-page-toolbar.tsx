import { useParams } from '@tanstack/react-router';
import { Search } from 'lucide-react';

import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { RefreshButton } from '@/components/ui/refresh-button';
import type { Schema } from '@/orval/models';
import { useGetSchemas } from '@/orval/schemas';

interface SchemasPageToolbarProps {
  schemas: Schema[];
  isFetchingSchemas: boolean;
}

export function SchemasPageToolbar({ schemas }: SchemasPageToolbarProps) {
  const { databaseName } = useParams({
    from: '/databases/$databaseName/schemas/',
  });

  const { refetch: refetchSchemas, isFetching: isFetchingSchemas } = useGetSchemas(databaseName);

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
          <Input disabled placeholder="Search" />
        </InputRoot>
        <RefreshButton isDisabled={isFetchingSchemas} onRefresh={refetchSchemas} />
      </div>
    </div>
  );
}
