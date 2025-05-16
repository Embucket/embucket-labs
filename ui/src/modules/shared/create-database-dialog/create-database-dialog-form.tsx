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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import type { Volume } from '@/orval/models';

const schema = z.object({
  name: z.string(),
  volumeName: z.string(),
});

interface CreateDatabaseDialogForm {
  onSubmit: (data: z.infer<typeof schema>) => void;
  volumes: Volume[];
}

export const CreateDatabaseDialogForm = ({ onSubmit, volumes }: CreateDatabaseDialogForm) => {
  const form = useForm<z.infer<typeof schema>>({
    resolver: zodResolver(schema),
    defaultValues: {
      name: '',
      volumeName: '',
    },
  });

  return (
    <FormProvider {...form}>
      <form
        id="createDatabaseDialogForm"
        onSubmit={form.handleSubmit(onSubmit)}
        className="flex flex-col gap-2"
      >
        <FormField
          control={form.control}
          name="name"
          render={({ field }) => (
            <FormItem>
              <FormLabel>Database Name</FormLabel>
              <FormControl>
                <Input {...field} type="name" required />
              </FormControl>
              <FormMessage />
            </FormItem>
          )}
        />
        <FormField
          control={form.control}
          name="volumeName"
          render={({ field }) => (
            <FormItem>
              <FormLabel>Volume</FormLabel>
              <Select {...field}>
                <SelectTrigger className="w-full bg-transparent!">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {volumes.map((volume) => (
                    <SelectItem key={volume.name} value={volume.name}>
                      {volume.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </FormItem>
          )}
        />
      </form>
    </FormProvider>
  );
};
