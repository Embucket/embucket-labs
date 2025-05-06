import type { TableColumn } from '@/orval/models';

export const TABLE_COLUMNS_MOCK: TableColumn[] = [
  { name: 'ID', type: 'VARCHAR(255)', nullable: 'true', default: 'NULL', description: '' },
  { name: 'EMAIL', type: 'VARCHAR(255)', nullable: 'true', default: 'NULL', description: '' },
  { name: 'AMOUNT', type: 'NUMBER(38, 10)', nullable: 'true', default: 'NULL', description: '' },
  { name: 'DATE', type: 'DATE', nullable: 'true', default: 'NULL', description: '' },
  { name: 'CREATED_AT', type: 'TIMESTAMP', nullable: 'true', default: 'NULL', description: '' },
  { name: 'UPDATED_AT', type: 'TIMESTAMP', nullable: 'true', default: 'NULL', description: '' },
  { name: 'DELETED_AT', type: 'TIMESTAMP', nullable: 'true', default: 'NULL', description: '' },
  { name: 'IS_DELETED', type: 'BOOLEAN', nullable: 'true', default: 'NULL', description: '' },
];
