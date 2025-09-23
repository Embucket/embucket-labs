import type { Cell } from '@tanstack/react-table';
import { flexRender, getCoreRowModel, useReactTable, type ColumnDef } from '@tanstack/react-table';

import { Skeleton } from '@/components/ui/skeleton';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { cn } from '@/lib/utils';
import { makeCellId } from '@/modules/editor/editor-utils';

function DataTableLoadingRows({ columnsLength }: { columnsLength: number }) {
  const rowsLength = 9;
  const defaultColumnsLength = 5;

  return (
    <>
      {Array.from({ length: rowsLength }).map((_, rowIndex) => (
        <TableRow key={rowIndex}>
          {Array.from({ length: columnsLength || defaultColumnsLength }).map((_, colIndex) => (
            <TableCell key={colIndex}>
              <Skeleton className="h-5 w-full" />
            </TableCell>
          ))}
        </TableRow>
      ))}
    </>
  );
}

interface DataTableProps<T> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  columns: ColumnDef<T, any>[];
  data: T[];
  onRowClick?: (row: T) => void;
  onCellClick?: (cell: Cell<T, unknown>) => void;
  selectedCellId?: string;
  isLoading: boolean;
  removeLRBorders?: boolean;
  rounded?: boolean;
}

// TODO: Double check index keys
export function DataTable<T>({
  columns,
  data,
  onRowClick,
  onCellClick,
  isLoading,
  removeLRBorders,
  rounded,
  selectedCellId,
}: DataTableProps<T>) {
  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
  });

  return (
    <Table removeLRBorders={removeLRBorders} rounded={rounded}>
      <TableHeader>
        {table.getHeaderGroups().map((headerGroup) => (
          <TableRow key={headerGroup.id} className="text-nowrap hover:bg-inherit">
            {headerGroup.headers.map((header) => (
              <TableHead key={header.id} className={header.column.columnDef.meta?.headerClassName}>
                {flexRender(header.column.columnDef.header, header.getContext())}
              </TableHead>
            ))}
          </TableRow>
        ))}
      </TableHeader>
      <TableBody>
        {table.getRowModel().rows.map((row) => (
          <TableRow
            className={cn('text-left text-nowrap', {
              'hover:bg-hover cursor-pointer': Boolean(onRowClick),
            })}
            onClick={() => onRowClick?.(row.original)}
            key={row.id}
            data-state={row.getIsSelected() && 'selected'}
          >
            {row.getVisibleCells().map((cell) => {
              const cellId = makeCellId(row.id, cell.column.id);

              return (
                <TableCell
                  key={cellId}
                  className={cn(
                    cell.column.columnDef.meta?.cellClassName,
                    Boolean(onCellClick) && 'hover:bg-hover cursor-pointer',
                    selectedCellId === cellId && 'bg-hover',
                  )}
                  data-cell-id={cellId}
                  onClick={(e) => {
                    if (onCellClick) {
                      e.stopPropagation();
                      onCellClick(cell);
                    }
                  }}
                >
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </TableCell>
              );
            })}
          </TableRow>
        ))}
        {isLoading && <DataTableLoadingRows columnsLength={columns.length} />}
      </TableBody>
    </Table>
  );
}
