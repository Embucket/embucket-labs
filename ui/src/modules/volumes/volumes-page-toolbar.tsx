import { useState } from 'react';

import { Search } from 'lucide-react';

import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { RefreshButton } from '@/components/ui/refresh-button';
import { useDebounce } from '@/hooks/use-debounce';
import type { Volume } from '@/orval/models';
import { useGetVolumes } from '@/orval/volumes';

interface VolumesPageToolbarProps {
  volumes: Volume[];
  isFetchingVolumes: boolean;
}

export function VolumesPageToolbar({ volumes }: VolumesPageToolbarProps) {
  const [search, setSearch] = useState('');
  const debouncedSearch = useDebounce(search, 300);

  const { refetch: refetchVolumes, isFetching: isFetchingVolumes } = useGetVolumes({
    search: debouncedSearch,
  });

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
          <Input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search" />
        </InputRoot>
        <RefreshButton isDisabled={isFetchingVolumes} onRefresh={refetchVolumes} />
      </div>
    </div>
  );
}
