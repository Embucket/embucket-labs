import { useState } from 'react';

import { Box } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { useGetVolumes } from '@/orval/volumes';

import { CreateVolumeDialog } from '../shared/create-volume-dialog/create-volume-dialog';
import { DataPageEmptyContainer } from '../shared/data-page/data-page-empty-container';
import { DataPageHeader } from '../shared/data-page/data-page-header';
import { DataPageScrollArea } from '../shared/data-page/data-page-scroll-area';
import { VolumesTable } from './volumes-page-table';
import { VolumesPageToolbar } from './volumes-page-toolbar';

// TODO: Not a data page
export function VolumesPage() {
  const [opened, setOpened] = useState(false);

  const { data: { items: volumes } = {}, isFetching } = useGetVolumes();

  return (
    <>
      <DataPageHeader
        title="Volumes"
        Action={
          <Button disabled={isFetching} onClick={() => setOpened(true)}>
            Add Volume
          </Button>
        }
      />
      {!volumes?.length ? (
        <DataPageEmptyContainer
          Icon={Box}
          title="No Volumes Found"
          description="No volumes have been created yet. Create a volume to get started."
        />
      ) : (
        <>
          <VolumesPageToolbar volumes={volumes} isFetchingVolumes={isFetching} />
          <DataPageScrollArea>
            <VolumesTable volumes={volumes} isLoading={isFetching} />
          </DataPageScrollArea>
        </>
      )}
      <CreateVolumeDialog opened={opened} onSetOpened={setOpened} />
    </>
  );
}
