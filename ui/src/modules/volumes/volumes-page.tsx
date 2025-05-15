import { Box } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { useGetVolumes } from '@/orval/volumes';

import { PageHeader } from '../shared/page/page-header';
import { VolumesTable } from './volumes-page-table';

export function VolumesPage() {
  const { data: { items: volumes } = {}, isFetching } = useGetVolumes();

  return (
    <>
      <PageHeader title="Volumes" />
      <ScrollArea className="h-[calc(100vh-65px-32px)] p-4">
        <div className="flex size-full flex-col">
          {volumes?.length ? (
            <ScrollArea tableViewport>
              <VolumesTable volumes={volumes} isLoading={isFetching} />
              <ScrollBar orientation="horizontal" />
            </ScrollArea>
          ) : (
            <EmptyContainer
              // TODO: Hardcode
              className="min-h-[calc(100vh-32px-65px-32px)]"
              Icon={Box}
              title="No Volumes Found"
              description="No volumes have been created yet. Create a volume to get started."
            />
          )}
        </div>
        <ScrollBar orientation="vertical" />
      </ScrollArea>
    </>
  );
}
