import type { LucideIcon } from 'lucide-react';

interface PageHeaderProps {
  title: React.ReactNode;
  Icon?: LucideIcon;
  Action?: React.ReactNode;
}

export const PageHeader = ({ title, Action, Icon }: PageHeaderProps) => {
  return (
    <div className="flex min-h-[65px] border-b p-4">
      <div className="flex w-full items-center justify-between">
        <div className="flex items-center gap-2">
          {Icon && <Icon className="text-muted-foreground size-5" />}
          {typeof title === 'string' ? <h1 className="text-lg">{title}</h1> : title}
        </div>
        {Action}
      </div>
    </div>
  );
};
