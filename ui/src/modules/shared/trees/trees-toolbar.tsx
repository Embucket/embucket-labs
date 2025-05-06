import { useState } from 'react';

import { RefreshCw, Search } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { SidebarGroup } from '@/components/ui/sidebar';
import { cn } from '@/lib/utils';

interface TreesToolbarProps {
  isFetchingNavigationTrees: boolean;
  onRefetchNavigationTrees: () => void;
}

export const TreesToolbar = ({
  isFetchingNavigationTrees,
  onRefetchNavigationTrees,
}: TreesToolbarProps) => {
  const [isSpinning, setIsSpinning] = useState(false);

  const handleRefresh = async () => {
    setIsSpinning(true);
    await new Promise((resolve) => setTimeout(resolve, 500));
    onRefetchNavigationTrees();
    setIsSpinning(false);
  };

  return (
    <SidebarGroup className="px-4 pt-4">
      <div className="justify flex items-center justify-between gap-2">
        <InputRoot className="w-full">
          <InputIcon>
            <Search />
          </InputIcon>
          <Input disabled placeholder="Search" />
        </InputRoot>
        <Button
          disabled={isSpinning || isFetchingNavigationTrees}
          onClick={handleRefresh}
          size="icon"
          variant="ghost"
          className="text-muted-foreground size-8"
        >
          <RefreshCw className={cn((isSpinning || isFetchingNavigationTrees) && 'animate-spin')} />
        </Button>
      </div>
    </SidebarGroup>
  );
};
