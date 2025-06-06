import type { TablePreviewDataColumn } from '@/orval/models';

interface GroupedColumn {
  name: string;
  rows: { data: string }[];
}

export function groupDataPreviewTableColumnsByName(
  columns: TablePreviewDataColumn[],
): GroupedColumn[] {
  const columnData = new Map<string, GroupedColumn>();

  for (const column of columns) {
    if (!columnData.has(column.name)) {
      columnData.set(column.name, { name: column.name, rows: [] });
    }
    columnData.get(column.name)!.rows.push(...column.rows);
  }

  return Array.from(columnData.values());
}

export function generateDataPreviewTableRows(
  groupedColumns: GroupedColumn[],
): Record<string, string>[] {
  const rows: Record<string, string>[] = [];

  if (groupedColumns.length > 0) {
    const numberOfRows = Math.max(...groupedColumns.map((c) => c.rows.length));

    for (let i = 0; i < numberOfRows; i += 1) {
      const row: Record<string, string> = {};

      for (const column of groupedColumns) {
        row[column.name] = column.rows[i]?.data ?? '';
      }

      rows.push(row);
    }
  }

  return rows;
}
