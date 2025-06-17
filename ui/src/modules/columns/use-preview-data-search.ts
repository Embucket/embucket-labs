import { useMemo } from 'react';

import type { TablePreviewDataColumn } from '@/orval/models';

interface UsePreviewDataSearchProps {
  previewData?: TablePreviewDataColumn[];
  search: string;
}

export function usePreviewDataSearch({ previewData, search }: UsePreviewDataSearchProps) {
  const searchedPreviewData = useMemo(() => {
    if (!previewData?.length) return previewData ?? [];
    if (!search) return previewData;

    const searchLower = search.toLowerCase();
    return previewData.map((column) => ({
      ...column,
      rows: column.rows.filter((row) => row.data.toLowerCase().includes(searchLower)),
    }));
  }, [previewData, search]);

  const isEmpty = !searchedPreviewData.length || !searchedPreviewData[0].rows.length;
  const isEmptyDueToSearch = search.length > 0 && isEmpty;

  return {
    searchedPreviewData,
    isEmpty,
    isEmptyDueToSearch,
  };
}
