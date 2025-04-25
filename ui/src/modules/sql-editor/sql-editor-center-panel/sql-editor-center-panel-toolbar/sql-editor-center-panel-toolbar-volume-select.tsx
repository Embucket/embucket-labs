import { useEffect, useState } from 'react';

import { Database } from 'lucide-react';

import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { useGetVolumes } from '@/orval/volumes';

export const SqlEditorCenterPanelToolbarVolumeSelect = () => {
  const [selectedOption, setSelectedOption] = useState<string | undefined>();
  const { data: { items: volumes } = {}, isPending } = useGetVolumes();

  useEffect(() => {
    if (volumes?.length && !isPending && !selectedOption) {
      setSelectedOption(volumes[0].name);
    }
  }, [volumes, selectedOption, isPending]);

  return (
    <Select
      value={selectedOption}
      onValueChange={(value) => {
        setSelectedOption(value); // No error now, as the type matches
      }}
      disabled
    >
      <SelectTrigger className="hover:bg-sidebar-secondary-accent! h-8! border-none bg-transparent! outline-0">
        <div className="flex items-center gap-2">
          <Database className="size-4" />
          <SelectValue />
        </div>
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          {volumes?.map((volume) => (
            <SelectItem key={volume.name} value={volume.name}>
              {volume.name}
            </SelectItem>
          ))}
        </SelectGroup>
      </SelectContent>
    </Select>
  );
};
