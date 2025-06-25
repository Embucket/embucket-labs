import { useCallback, useMemo } from 'react';

import { useInfiniteQuery } from '@tanstack/react-query';

import type {
  TablePreviewDataColumn,
  TablePreviewDataResponse,
  TablePreviewDataRow,
} from '@/orval/models';
import { getGetTablePreviewDataQueryKey, getTablePreviewData } from '@/orval/tables';

const DEFAULT_PAGE_SIZE = 25;

interface UseInfinitePreviewDataProps {
  databaseName: string;
  schemaName: string;
  tableName: string;
  pageSize?: number;
  enabled?: boolean;
}

// Helper function to safely get rows from a column
const getColumnRows = (column: TablePreviewDataColumn): TablePreviewDataRow[] =>
  Array.isArray(column.rows) ? column.rows : [];

// Helper function to safely get items from a page
const getPageItems = (page: TablePreviewDataResponse): TablePreviewDataColumn[] =>
  Array.isArray(page.items) ? page.items : [];

export function useGetInfiniteTablePreviewData({
  databaseName,
  schemaName,
  tableName,
  pageSize = DEFAULT_PAGE_SIZE,
  enabled = true,
}: UseInfinitePreviewDataProps) {
  const {
    data,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
    isLoading,
    isFetching,
    error,
    refetch,
  } = useInfiniteQuery({
    queryKey: getGetTablePreviewDataQueryKey(databaseName, schemaName, tableName, {
      limit: pageSize,
    }),
    queryFn: async ({ pageParam = 0 }) =>
      getTablePreviewData(databaseName, schemaName, tableName, {
        limit: pageSize,
        offset: pageParam,
      }),
    initialPageParam: 0,
    enabled,
    getNextPageParam: (lastPage, allPages) => {
      const items = getPageItems(lastPage);
      if (items.length === 0) return;

      // If the first column has less rows than the page size, we've reached the end
      const firstColumnRows = getColumnRows(items[0]);
      if (firstColumnRows.length < pageSize) return;

      // Return the total number of rows fetched so far as the next offset
      return allPages.reduce((total, page) => {
        const pageItems = getPageItems(page);
        return total + (pageItems[0] ? getColumnRows(pageItems[0]).length : 0);
      }, 0);
    },
  });

  // Flatten all pages into a single array of columns
  const tableDataPreviewColumns = useMemo(() => {
    if (!data?.pages) return [];

    const tableDataPreviewColumnsMap = new Map<string, TablePreviewDataColumn>();

    data.pages.forEach((page) => {
      getPageItems(page).forEach((column) => {
        const existingColumn = tableDataPreviewColumnsMap.get(column.name);
        const columnRows = getColumnRows(column);

        if (existingColumn) {
          existingColumn.rows.push(...columnRows);
        } else {
          tableDataPreviewColumnsMap.set(column.name, {
            name: column.name,
            rows: [...columnRows],
          });
        }
      });
    });

    return Array.from(tableDataPreviewColumnsMap.values());
  }, [data]);

  const loadMore = useCallback(() => {
    if (hasNextPage && !isFetchingNextPage) {
      fetchNextPage();
    }
  }, [fetchNextPage, hasNextPage, isFetchingNextPage]);

  return {
    data: tableDataPreviewColumns,
    isLoading,
    isFetching,
    isFetchingNextPage,
    hasNextPage,
    error,
    loadMore,
    refetch,
  };
}
