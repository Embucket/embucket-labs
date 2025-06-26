import { useState } from 'react';

import { useParams } from '@tanstack/react-router';
import { Database, FolderTree } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { useDebounce } from '@/hooks/use-debounce';
import { useGetSchemas } from '@/orval/schemas';

import { CreateSchemaDialog } from '../shared/create-schema-dialog/create-schema-dialog';
import { PageEmptyContainer } from '../shared/page/page-empty-container';
import { PageHeader } from '../shared/page/page-header';
import { PageScrollArea } from '../shared/page/page-scroll-area';
import { SchemasTable } from './schemas-page-table';
import { SchemasPageToolbar } from './schemas-page-toolbar';

export function SchemasPage() {
  const [opened, setOpened] = useState(false);
  const [search, setSearch] = useState('');
  const debouncedSearch = useDebounce(search, 300);

  const { databaseName } = useParams({
    from: '/databases/_dataPagesLayout/$databaseName/schemas/',
  });
  const {
    data: { items: schemas } = {},
    isFetching: isFetchingSchemas,
    isLoading: isLoadingSchemas,
    refetch: refetchSchemas,
  } = useGetSchemas(databaseName, {
    search: debouncedSearch,
  });

  return (
    <>
      <PageHeader
        title={databaseName}
        Icon={Database}
        Action={
          <Button size="sm" disabled={isFetchingSchemas} onClick={() => setOpened(true)}>
            Add Schema
          </Button>
        }
      />

      <SchemasPageToolbar
        search={search}
        onSetSearch={setSearch}
        schemas={schemas ?? []}
        isFetchingSchemas={isFetchingSchemas}
        onRefetchSchemas={refetchSchemas}
      />
      {!schemas?.length && !isLoadingSchemas ? (
        <PageEmptyContainer
          Icon={FolderTree}
          variant="toolbar"
          title="No Schemas Found"
          description="No schemas have been found for this database."
        />
      ) : (
        <PageScrollArea>
          <SchemasTable isLoading={isLoadingSchemas} schemas={schemas ?? []} />
        </PageScrollArea>
      )}

      <CreateSchemaDialog opened={opened} onSetOpened={setOpened} databaseName={databaseName} />
    </>
  );
}
