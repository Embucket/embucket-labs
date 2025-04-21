import { zodResolver } from '@hookform/resolvers/zod';
import { useForm } from 'react-hook-form';
import { useTranslation } from 'react-i18next';
import { z } from 'zod';

// import { UserAvatar } from '@/app/layout/sidebar/user-avatar';
import {
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
  FormProvider,
} from '@/components/ui/form';
import { Input } from '@/components/ui/input';

const FORM_ID = 'profileSettingsForm';

const schema = z.object({
  email: z.string().email(),
  name: z.string(),
});

interface ProfileSettingsFormProps {
  isLoading: boolean;
  onSubmit: (data: z.infer<typeof schema>) => void;
}

export const ProfileSettingsForm = ({ onSubmit, isLoading }: ProfileSettingsFormProps) => {
  const { t } = useTranslation('profileSettings');

  const form = useForm<z.infer<typeof schema>>({
    resolver: zodResolver(schema),
    defaultValues: {
      email: 'test@email.com',
      name: 'Test Name',
    },
  });

  return (
    <div>
      <FormProvider {...form}>
        <form id={FORM_ID} onSubmit={form.handleSubmit(onSubmit)} className="flex flex-col gap-2">
          <FormField
            control={form.control}
            disabled={isLoading}
            name="email"
            render={({ field }) => (
              <FormItem>
                <FormLabel>{t('email.title')}</FormLabel>
                <FormDescription>{t('email.description')}</FormDescription>
                <FormControl>
                  <Input {...field} type="name" required />
                </FormControl>
                <FormMessage />
              </FormItem>
            )}
          />
          <FormField
            control={form.control}
            disabled={isLoading}
            name="name"
            render={({ field }) => (
              <FormItem>
                <FormLabel>{t('name.title')}</FormLabel>
                <FormDescription>{t('name.description')}</FormDescription>
                <FormControl>
                  <Input {...field} type="name" required />
                </FormControl>
                <FormMessage />
              </FormItem>
            )}
          />
          <FormItem>
            <FormLabel>{t('avatar.title')}</FormLabel>
            <FormDescription>{t('avatar.description')}</FormDescription>
            <FormControl>{/* <UserAvatar /> */}</FormControl>
            <FormMessage />
          </FormItem>
        </form>
      </FormProvider>
    </div>
  );
};
