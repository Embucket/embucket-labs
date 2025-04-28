import { Share2 } from 'lucide-react';

import { Button } from '@/components/ui/button';

export function SqlEditorCenterPanelToolbarShareButton() {
  return (
    <Button disabled size="icon" variant="ghost" className="text-muted-foreground size-8">
      <Share2 />
    </Button>
  );
}
