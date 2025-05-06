import { Search } from 'lucide-react';

import { Input, InputIcon, InputRoot } from '@/components/ui/input';

interface DataPageHeaderProps {
  title: string;
  Action?: React.ReactNode;
  secondaryText?: string;
}

export const DataPageHeader = ({ title, secondaryText, Action }: DataPageHeaderProps) => {
  return (
    <div className="border-b p-4">
      <div className="mb-4 flex items-center justify-between">
        <h1 className="text-lg">{title}</h1>
        {Action}
      </div>
      <div className="flex items-center justify-between gap-4">
        <p className="text-muted-foreground text-sm text-nowrap">{secondaryText}</p>
        <InputRoot>
          <InputIcon>
            <Search />
          </InputIcon>
          <Input className="min-w-80" disabled placeholder="Search" />
        </InputRoot>
      </div>
    </div>
  );
};
