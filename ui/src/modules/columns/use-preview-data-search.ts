import { useMemo } from 'react';

import type { TablePreviewDataColumn } from '@/orval/models';

interface UsePreviewDataSearchProps {
  previewData?: TablePreviewDataColumn[];
  search: string;
}

export function usePreviewDataSearch({ previewData, search }: UsePreviewDataSearchProps) {
  const searchedPreviewData = useMemo(() => {
    if (!previewData?.length || !search) {
      return previewData ?? [];
    }

    const searchLower = search.toLowerCase();

    const isColumnNameMatch = previewData.some((column) =>
      column.name.toLowerCase().includes(searchLower),
    );

    if (isColumnNameMatch) {
      return previewData;
    }

    const matchingRowIndices = new Set<number>();

    previewData.forEach((column) => {
      column.rows.forEach((row, index) => {
        if (row.data.toLowerCase().includes(searchLower)) {
          matchingRowIndices.add(index);
        }
      });
    });

    return previewData.map((column) => ({
      ...column,
      rows: column.rows.filter((_, index) => matchingRowIndices.has(index)),
    }));
  }, [previewData, search]);

  const isEmpty = !!search && searchedPreviewData.every((column) => column.rows.length === 0);
  const isEmptyDueToSearch = search.length > 0 && isEmpty;

  return {
    searchedPreviewData,
    isEmpty,
    isEmptyDueToSearch,
  };
}
