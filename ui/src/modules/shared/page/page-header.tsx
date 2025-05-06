import { Search } from 'lucide-react';

import { Input, InputIcon, InputRoot } from '@/components/ui/input';

interface PageHeaderProps {
  title: string;
  children?: React.ReactNode;
}

export const PageHeader = ({ title, children }: PageHeaderProps) => {
  return (
    <div className="flex items-center justify-between border-b p-4">
      <div className="flex items-center gap-2">
        <h1 className="text-xl font-semibold">{title}</h1>
        {children}
      </div>
      <InputRoot>
        <InputIcon>
          <Search />
        </InputIcon>
        <Input className="min-w-80" disabled placeholder="Search" />
      </InputRoot>
    </div>
  );
};
