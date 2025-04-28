import type { Worksheet } from '@/orval/models';

export const WORKSHEETS_MOCK: Worksheet[] = [
  {
    content: 'SELECT * FROM users',
    createdAt: '2023-10-01T12:00:00Z',
    id: 1,
    name: 'Users',
    updatedAt: '2023-10-01T12:00:05Z',
  },
  {
    content: 'SELECT * FROM orders',
    createdAt: '2023-10-01T12:05:00Z',
    id: 2,
    name: 'Orders',
    updatedAt: '2023-10-01T12:05:10Z',
  },
];
