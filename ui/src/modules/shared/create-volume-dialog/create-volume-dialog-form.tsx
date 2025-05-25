import type React from 'react';
import { useEffect } from 'react';

import { zodResolver } from '@hookform/resolvers/zod';
import { useForm } from 'react-hook-form';
import { z } from 'zod';

import {
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
  FormProvider,
} from '@/components/ui/form';
import { Input } from '@/components/ui/input';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { cn } from '@/lib/utils';
import {
  VolumeTypeOneOfAllOfType,
  VolumeTypeOneOfFourAllOfType,
  VolumeTypeOneOfOnezeroType,
  VolumeTypeOneOfSevenAllOfType,
} from '@/orval/models';

const schema = z
  .object({
    name: z.string().min(1, 'Volume name is required'),
    // TODO: This naming is a joke
    type: z.enum([
      VolumeTypeOneOfSevenAllOfType.file,
      VolumeTypeOneOfOnezeroType.memory,
      VolumeTypeOneOfAllOfType.s3,
      VolumeTypeOneOfFourAllOfType.s3Tables,
    ]),
    path: z.string().optional(),
  })
  .superRefine((data, ctx) => {
    if (data.type === 'file' && (!data.path || data.path.trim() === '')) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'Path is required for File Volume',
        path: ['path'],
      });
    }
  });

interface TypeOptionProps {
  id: string;
  value: string;
  title: string;
  description: string;
  selectedValue: string;
  radioControl: React.ReactNode;
  disabled?: boolean;
}

const TypeOption: React.FC<TypeOptionProps> = ({
  id,
  value,
  title,
  description,
  selectedValue,
  radioControl,
  disabled,
}) => {
  return (
    <FormItem
      className={cn(
        'rounded-md border transition-colors',
        selectedValue === value && !disabled && 'border-primary',
        disabled && 'cursor-not-allowed opacity-50',
      )}
    >
      <FormLabel
        htmlFor={id}
        className={cn(
          'flex flex-row items-start space-x-2 p-3',
          disabled ? 'cursor-not-allowed' : 'cursor-pointer',
        )}
      >
        {radioControl}
        <div className="flex flex-col gap-1">
          <span className="font-semibold">{title}</span>
          <p className="text-muted-foreground text-xs font-light">{description}</p>
        </div>
      </FormLabel>
    </FormItem>
  );
};

interface CreateVolumeDialogForm {
  onSubmit: (data: z.infer<typeof schema>) => void;
}

export const CreateVolumeDialogForm = ({ onSubmit }: CreateVolumeDialogForm) => {
  const form = useForm<z.infer<typeof schema>>({
    resolver: zodResolver(schema),
    defaultValues: {
      name: '',
      type: 'file',
      path: '',
    },
  });

  const type = form.watch('type');

  useEffect(() => {
    if (type !== 'file') {
      form.clearErrors('path');
    }
  }, [type, form]);

  const volumeOptions = [
    {
      id: 'memoryVolume',
      value: 'memory',
      title: 'Memory Volume',
      description: 'In-memory storage for fast access.',
    },
    {
      id: 'fileVolume',
      value: 'file',
      title: 'File Volume',
      description: 'Persistent storage using local disk file.',
    },
    {
      id: 's3Volume',
      value: 's3',
      title: 'S3 Volume',
      description: 'Cloud-based storage with AWS S3.',
      disabled: true,
    },
  ];

  return (
    <FormProvider {...form}>
      <form
        id="createVolumeDialogForm"
        onSubmit={form.handleSubmit(onSubmit)}
        className="flex flex-col gap-4"
      >
        <FormField
          control={form.control}
          name="name"
          render={({ field }) => (
            <FormItem>
              <FormLabel>Volume Name</FormLabel>
              <FormControl>
                <Input {...field} type="name" required />
              </FormControl>
              <FormMessage />
            </FormItem>
          )}
        />
        <FormField
          control={form.control}
          name="type"
          render={({ field }) => (
            <FormItem>
              <FormLabel>Specify volume type</FormLabel>
              <FormControl>
                <RadioGroup
                  onValueChange={field.onChange}
                  defaultValue={field.value}
                  className="grid grid-cols-3 gap-2"
                >
                  {volumeOptions.map((option) => (
                    <TypeOption
                      key={option.id}
                      id={option.id}
                      value={option.value}
                      title={option.title}
                      description={option.description}
                      selectedValue={field.value}
                      disabled={option.disabled}
                      radioControl={
                        <FormControl>
                          <RadioGroupItem
                            value={option.value}
                            id={option.id}
                            disabled={option.disabled}
                          />
                        </FormControl>
                      }
                    />
                  ))}
                </RadioGroup>
              </FormControl>
              <FormMessage />
            </FormItem>
          )}
        />
        {type === 'file' && (
          <FormField
            control={form.control}
            name="path"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Path</FormLabel>
                <FormControl>
                  <Input {...field} required />
                </FormControl>
                <FormMessage />
              </FormItem>
            )}
          />
        )}
      </form>
    </FormProvider>
  );
};
