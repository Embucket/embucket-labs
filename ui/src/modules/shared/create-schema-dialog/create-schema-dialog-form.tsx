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

const schema = z.object({
  name: z.string(),
});

interface CreateSchemaDialogForm {
  onSubmit: (data: z.infer<typeof schema>) => void;
}

export const CreateSchemaDialogForm = ({ onSubmit }: CreateSchemaDialogForm) => {
  const form = useForm<z.infer<typeof schema>>({
    resolver: zodResolver(schema),
    defaultValues: {
      name: '',
    },
  });

  return (
    <FormProvider {...form}>
      <form
        id="createSchemaDialogForm"
        onSubmit={form.handleSubmit(onSubmit)}
        className="flex flex-col gap-2"
      >
        <FormField
          control={form.control}
          name="name"
          render={({ field }) => (
            <FormItem>
              <FormLabel>Schema Name</FormLabel>
              <FormControl>
                <Input {...field} type="name" required />
              </FormControl>
              <FormMessage />
            </FormItem>
          )}
        />
      </form>
    </FormProvider>
  );
};
