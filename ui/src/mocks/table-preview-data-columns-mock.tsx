import type { TablePreviewDataColumn } from '@/orval/models';

export const TABLE_PREVIEW_DATA_COLUMNS_MOCK: TablePreviewDataColumn[] = [
  {
    name: 'coalesce(mydb1.myschema10.mytable10.id,Utf8("Unsupported"))',
    rows: Array.from({ length: 20 }, (_, i) => ({
      data: (i + 1).toString(),
    })),
  },
  {
    name: 'coalesce(mydb2.myschema10.mytable10.name,Utf8("Unsupported"))',
    rows: Array.from({ length: 20 }, (_, i) => ({
      data: `Name${(i + 1).toString()}`,
    })),
  },
  {
    name: 'coalesce(mydb3.myschema10.mytable10.name,Utf8("Unsupported"))',
    rows: Array.from({ length: 20 }, (_, i) => ({
      data: `Name${(i + 1).toString()}`,
    })),
  },
  {
    name: 'coalesce(mydb4.myschema10.mytable10.name,Utf8("Unsupported"))',
    rows: Array.from({ length: 20 }, (_, i) => ({
      data: `Name${(i + 1).toString()}`,
    })),
  },
];
