import dayjs from './dayjs';

export function formatTime(date: string) {
  const startTime = dayjs(date);
  const diff = dayjs().diff(startTime, 'hour');
  return diff < 24 ? dayjs(startTime).fromNow() : dayjs(startTime).format('DD/MM/YYYY HH:mm');
}
