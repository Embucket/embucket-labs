/* eslint-disable @typescript-eslint/no-unused-vars */
import '@tanstack/react-table';

import type { RowData } from '@tanstack/react-table';

declare module '@tanstack/react-table' {
  interface ColumnMeta<TData extends RowData, TValue> {
    headerClassName?: string;
    cellClassName?: string;
  }
}
