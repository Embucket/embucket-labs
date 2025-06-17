import { useState } from 'react';

import { Database } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from '@/components/ui/resizable';
import { useDebounce } from '@/hooks/use-debounce';
import { useGetDatabases } from '@/orval/databases';
import { useGetVolumes } from '@/orval/volumes';

import { CreateDatabaseDialog } from '../shared/create-database-dialog/create-database-dialog';
import { DataPageTrees } from '../shared/data-page/data-page-trees';
import { PageEmptyContainer } from '../shared/page/page-empty-container';
import { PageHeader } from '../shared/page/page-header';
import { PageScrollArea } from '../shared/page/page-scroll-area';
import { DatabasesTable } from './databases-page-table';
import { DatabasesPageToolbar } from './databases-page-toolbar';

export function DatabasesPage() {
  const [opened, setOpened] = useState(false);
  const [search, setSearch] = useState('');
  const debouncedSearch = useDebounce(search, 300);

  const {
    data: { items: databases } = {},
    isLoading: isLoadingDatabases,
    isFetching: isFetchingDatabases,
    refetch: refetchDatabases,
  } = useGetDatabases({
    search: debouncedSearch,
  });
  const { data: { items: volumes } = {}, isFetching: isFetchingVolumes } = useGetVolumes({
    search: debouncedSearch,
  });

  return (
    <>
      <ResizablePanelGroup direction="horizontal">
        <ResizablePanel collapsible defaultSize={20} minSize={20} order={1}>
          <DataPageTrees />
        </ResizablePanel>
        <ResizableHandle withHandle />
        <ResizablePanel collapsible defaultSize={20} order={1}>
          <PageHeader
            title="Databases"
            Action={
              <Button
                size="sm"
                disabled={isFetchingDatabases || isFetchingVolumes || !volumes?.length}
                onClick={() => setOpened(true)}
              >
                Add Database
              </Button>
            }
          />

          <DatabasesPageToolbar
            search={search}
            onSetSearch={setSearch}
            onRefetchDatabases={refetchDatabases}
            databases={databases ?? []}
            isFetchingDatabases={isFetchingDatabases}
          />
          {!databases?.length && !isLoadingDatabases ? (
            <PageEmptyContainer
              Icon={Database}
              variant="toolbar"
              title="No Databases Found"
              description="No databases have been created yet. Create a database to get started."
            />
          ) : (
            <PageScrollArea>
              <DatabasesTable isLoading={isLoadingDatabases} databases={databases ?? []} />
            </PageScrollArea>
          )}
        </ResizablePanel>
      </ResizablePanelGroup>
      <CreateDatabaseDialog opened={opened} onSetOpened={setOpened} />
    </>
  );
}
