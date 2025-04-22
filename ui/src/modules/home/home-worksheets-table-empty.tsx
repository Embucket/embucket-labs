import { FileText } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';

export const HomeWorksheetsTableEmpty = () => {
  return (
    <EmptyContainer
      Icon={FileText}
      title="No Worksheets Created Yet"
      description="Create your first worksheet to start querying data"
      // eslint-disable-next-line @typescript-eslint/no-empty-function
      onCtaClick={() => {}}
      ctaText="Create Worksheet"
    />
  );
};
