import i18next, { type Resource } from 'i18next';
import { initReactI18next } from 'react-i18next';

import en from './locales/en';

const resources: Resource = {
  en,
} as const;

i18next.use(initReactI18next).init({
  resources,
  lng: 'en',
  interpolation: {
    escapeValue: false, // react already safes from xss
  },
});

export default i18next;
