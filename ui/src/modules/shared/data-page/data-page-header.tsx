import type { LucideIcon } from 'lucide-react';

interface DataPageHeaderProps {
  title: string;
  Icon?: LucideIcon;
  Action?: React.ReactNode;
}

export const DataPageHeader = ({ title, Action, Icon }: DataPageHeaderProps) => {
  return (
    <div className="border-b p-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          {Icon && <Icon className="text-muted-foreground size-5" />}
          <h1 className="text-lg">{title}</h1>
        </div>
        {Action}
      </div>
    </div>
  );
};
