import { useState } from 'react';

import { RefreshCw } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

interface RefreshButtonProps {
  isDisabled?: boolean;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  onRefresh: () => Promise<any>;
  className?: string;
  spinDuration?: number;
}

export const RefreshButton = ({
  isDisabled = false,
  onRefresh,
  className,
  spinDuration = 500,
}: RefreshButtonProps) => {
  const [isSpinning, setIsSpinning] = useState(false);

  const handleRefresh = async () => {
    setIsSpinning(true);
    await new Promise((resolve) => setTimeout(resolve, spinDuration));
    await onRefresh();
    setIsSpinning(false);
  };

  return (
    <Button
      disabled={isSpinning || isDisabled}
      onClick={handleRefresh}
      size="icon"
      variant="ghost"
      className={cn('text-muted-foreground size-8', className)}
    >
      <RefreshCw className={cn((isSpinning || isDisabled) && 'animate-spin')} />
    </Button>
  );
};
