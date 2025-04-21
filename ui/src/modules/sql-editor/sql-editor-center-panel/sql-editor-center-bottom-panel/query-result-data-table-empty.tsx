import { TextSearch } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';

export const QueryResultDataTableEmpty = () => {
  return (
    <EmptyContainer
      Icon={TextSearch}
      title="No Results Yet"
      description="Once you run a query, results will be displayed here."
    />
  );
};
