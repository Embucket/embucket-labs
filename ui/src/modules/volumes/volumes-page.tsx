import { useState } from 'react';

import { Box } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { useGetVolumes } from '@/orval/volumes';

import { CreateVolumeDialog } from '../shared/create-volume-dialog/create-volume-dialog';
import { PageEmptyContainer } from '../shared/page/page-empty-container';
import { PageHeader } from '../shared/page/page-header';
import { PageScrollArea } from '../shared/page/page-scroll-area';
import { VolumesTable } from './volumes-page-table';
import { VolumesPageToolbar } from './volumes-page-toolbar';

export function VolumesPage() {
  const [opened, setOpened] = useState(false);

  const { data: { items: volumes } = {}, isFetching } = useGetVolumes();

  return (
    <>
      <PageHeader
        title="Volumes"
        Action={
          <Button size="sm" disabled={isFetching} onClick={() => setOpened(true)}>
            Add Volume
          </Button>
        }
      />
      {!volumes?.length ? (
        <PageEmptyContainer
          Icon={Box}
          title="No Volumes Found"
          description="No volumes have been created yet. Create a volume to get started."
        />
      ) : (
        <>
          <VolumesPageToolbar volumes={volumes} isFetchingVolumes={isFetching} />
          <PageScrollArea>
            <VolumesTable volumes={volumes} isLoading={isFetching} />
          </PageScrollArea>
        </>
      )}
      <CreateVolumeDialog opened={opened} onSetOpened={setOpened} />
    </>
  );
}
