import { Search } from 'lucide-react';

import { Input, InputIcon, InputRoot } from '@/components/ui/input';

export function HomePage() {
  return (
    <div className="flex items-center justify-between border-b p-4">
      <h1 className="text-xl font-semibold">Home</h1>
      <InputRoot>
        <InputIcon>
          <Search />
        </InputIcon>
        <Input disabled placeholder="Search" />
      </InputRoot>
    </div>
  );
}
