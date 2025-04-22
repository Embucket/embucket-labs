import { Search } from 'lucide-react';

import { Input, InputIcon, InputRoot } from '@/components/ui/input';

export function QueryHistoryPage() {
  return (
    <div className="flex items-center justify-between border-b p-4">
      <h1 className="text-xl font-semibold">Query History</h1>
      <InputRoot>
        <InputIcon>
          <Search />
        </InputIcon>
        <Input className="min-w-80" disabled placeholder="Search" />
      </InputRoot>
    </div>
  );
}
