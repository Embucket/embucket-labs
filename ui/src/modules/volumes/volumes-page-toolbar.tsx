import { Search } from 'lucide-react';

import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { RefreshButton } from '@/components/ui/refresh-button';
import type { Volume } from '@/orval/models';

interface VolumesPageToolbarProps {
  search: string;
  onSetSearch: (search: string) => void;
  volumes: Volume[];
  isFetchingVolumes: boolean;
  onRefetchVolumes: () => Promise<unknown>;
}

export function VolumesPageToolbar({
  search,
  onSetSearch,
  volumes,
  isFetchingVolumes,
  onRefetchVolumes,
}: VolumesPageToolbarProps) {
  return (
    <div className="flex items-center justify-between gap-4 p-4">
      <p className="text-muted-foreground text-sm text-nowrap">
        {volumes.length ? `${volumes.length} volumes found` : ''}
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
        <RefreshButton isDisabled={isFetchingVolumes} onRefresh={onRefetchVolumes} />
      </div>
    </div>
  );
}
