import { FileText } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';

export const HomeWorksheetsTableEmpty = () => {
  return (
    <EmptyContainer
      Icon={FileText}
      title="No SQL Worksheets Created Yet"
      description="Create your first worksheet to start querying data"
      // onCtaClick={() => {}}
      // ctaText="Create Worksheet"
    />
  );
};
