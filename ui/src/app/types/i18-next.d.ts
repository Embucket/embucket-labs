import type en from '@/app/i18n/locales/en';

import 'react-i18next';

declare module 'i18next' {
  interface CustomTypeOptions {
    resources: typeof en;
  }
}
