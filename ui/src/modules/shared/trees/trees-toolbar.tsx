import { Search } from 'lucide-react';

import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { RefreshButton } from '@/components/ui/refresh-button';
import { SidebarGroup } from '@/components/ui/sidebar';

interface TreesToolbarProps {
  isFetchingNavigationTrees: boolean;
  // TODO: ???
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  onRefetchNavigationTrees: () => Promise<any>;
}

export const TreesToolbar = ({
  isFetchingNavigationTrees,
  onRefetchNavigationTrees,
}: TreesToolbarProps) => {
  return (
    <SidebarGroup className="px-4 pt-4">
      <div className="justify flex items-center justify-between gap-2">
        <InputRoot className="w-full">
          <InputIcon>
            <Search />
          </InputIcon>
          <Input disabled placeholder="Search" />
        </InputRoot>
        <RefreshButton
          isDisabled={isFetchingNavigationTrees}
          onRefresh={onRefetchNavigationTrees}
        />
      </div>
    </SidebarGroup>
  );
};
