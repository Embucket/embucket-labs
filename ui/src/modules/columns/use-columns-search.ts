import { useMemo } from 'react';

import type { TableColumn } from '@/orval/models';

interface UseColumnsSearchProps {
  columns?: TableColumn[];
  search: string;
}

export function useColumnsSearch({ columns, search }: UseColumnsSearchProps) {
  const searchedColumns = useMemo(() => {
    if (!columns) return [];
    if (!search) return columns;

    const searchLower = search.toLowerCase();
    return columns.filter(
      (column) =>
        column.name.toLowerCase().includes(searchLower) ||
        column.type.toLowerCase().includes(searchLower),
    );
  }, [columns, search]);

  return {
    searchedColumns,
    isEmpty: !searchedColumns.length,
    isEmptyDueToSearch: search.length > 0 && !searchedColumns.length,
  };
}
