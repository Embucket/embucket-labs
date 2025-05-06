import { useState } from 'react';

import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectSeparator,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';

const CUSTOM_OPTION_VALUE = 'custom-option';

interface TableDataUploadDatabaseSelectProps {
  options: {
    value: string;
    label: string;
  }[];
  value?: string;
  onChange: (value: string) => void;
  placeholder?: string;
  disabled?: boolean;
  customOptionLabel?: string;
  onCustomOptionClick?: () => void;
}

export function TableDataUploadSelect({
  options,
  value,
  onChange,
  placeholder,
  disabled,
  customOptionLabel,
  onCustomOptionClick,
}: TableDataUploadDatabaseSelectProps) {
  const [selectedOption, setSelectedOption] = useState<string | undefined>(value);

  const handleValueChange = (newValue: string) => {
    if (newValue === CUSTOM_OPTION_VALUE && onCustomOptionClick) {
      onCustomOptionClick();
      setSelectedOption(newValue);
      return;
    }
    setSelectedOption(newValue);
    onChange(newValue);
  };

  return (
    <Select value={selectedOption} onValueChange={handleValueChange} disabled={disabled}>
      <SelectTrigger className="w-full">
        <SelectValue placeholder={placeholder} />
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          {options.map((option) => (
            <SelectItem key={option.value} value={option.value}>
              <span className="max-w-[100px] truncate">{option.label}</span>
            </SelectItem>
          ))}
        </SelectGroup>
        {customOptionLabel && onCustomOptionClick && (
          <>
            <SelectSeparator />
            <SelectGroup>
              <SelectItem value={CUSTOM_OPTION_VALUE} className="cursor-pointer">
                <span className="max-w-[100px] truncate">{customOptionLabel}</span>
              </SelectItem>
            </SelectGroup>
          </>
        )}
      </SelectContent>
    </Select>
  );
}
