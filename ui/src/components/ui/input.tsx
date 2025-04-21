import type * as React from 'react';

import { Slot } from '@radix-ui/react-slot';
import type { VariantProps } from 'class-variance-authority';
import { cva } from 'class-variance-authority';

import { cn } from '@/lib/utils';

export interface InputRootProps extends React.ComponentProps<'div'> {}

function InputRoot({ children, className, ...props }: InputRootProps) {
  return (
    <div className={cn('relative', className)} {...props}>
      {children}
    </div>
  );
}

function InputIcon({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <Slot
      role="presentation"
      className={cn(
        'text-muted-foreground pointer-events-none absolute top-2 bottom-2 left-3 size-4 [&~input]:pl-8',
        className,
      )}
    >
      {children}
    </Slot>
  );
}

const inputVariants = cva(
  'flex h-8 w-full rounded-md bg-transparent border px-3 py-1 text-base transition-colors file:border-0 file:bg-transparent file:text-sm file:font-medium file:text-foreground placeholder:text-muted-foreground focus-visible:outline-hidden focus-visible:ring-1 disabled:cursor-not-allowed disabled:opacity-50 md:text-sm',
  {
    variants: {
      variant: {
        default: 'border-input shadow-xs focus-visible:ring-ring',
        destructive: 'border-destructive shadow-xs focus-visible:ring-destructive',
        ghost: 'border-transparent -mx-3 -my-1 focus-visible:ring-ring',
      },
    },
    defaultVariants: {
      variant: 'default',
    },
  },
);

export interface InputProps
  extends React.ComponentProps<'input'>,
    VariantProps<typeof inputVariants> {
  ref?: React.ForwardedRef<HTMLInputElement>;
}

function Input({ className, type, ref, variant, ...props }: InputProps) {
  return (
    <input type={type} className={cn(inputVariants({ variant }), className)} ref={ref} {...props} />
  );
}

// eslint-disable-next-line react-refresh/only-export-components
export { Input, InputIcon, InputRoot, inputVariants };
