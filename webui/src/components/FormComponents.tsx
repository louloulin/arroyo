import React from 'react';
import {
  FormControl,
  FormLabel,
  FormErrorMessage,
  FormHelperText,
  Input,
  Select,
  NumberInput,
  NumberInputField,
  NumberInputStepper,
  NumberIncrementStepper,
  NumberDecrementStepper,
  Switch,
  Textarea,
  InputGroup,
  InputRightElement,
  Button,
  Tooltip,
  Box,
  Text,
  HStack,
  Icon,
} from '@chakra-ui/react';
import { InfoIcon } from '@chakra-ui/icons';

// 表单字段属性
interface FormFieldProps {
  label: string;
  name: string;
  error?: string;
  isRequired?: boolean;
  helperText?: string;
  tooltip?: string;
  isDisabled?: boolean;
  children: React.ReactNode;
}

// 表单字段组件
export const FormField: React.FC<FormFieldProps> = ({
  label,
  name,
  error,
  isRequired = false,
  helperText,
  tooltip,
  isDisabled = false,
  children,
}) => {
  return (
    <FormControl isInvalid={!!error} isRequired={isRequired} isDisabled={isDisabled} mb={4}>
      <HStack spacing={1} alignItems="center">
        <FormLabel htmlFor={name} mb={1}>
          {label}
        </FormLabel>
        {tooltip && (
          <Tooltip label={tooltip} placement="top" hasArrow>
            <Icon as={InfoIcon} color="gray.500" boxSize={4} />
          </Tooltip>
        )}
      </HStack>
      {children}
      {helperText && !error && <FormHelperText>{helperText}</FormHelperText>}
      {error && <FormErrorMessage>{error}</FormErrorMessage>}
    </FormControl>
  );
};

// 文本输入属性
interface TextInputProps {
  label: string;
  name: string;
  value: string;
  onChange: (value: string) => void;
  error?: string;
  placeholder?: string;
  isRequired?: boolean;
  helperText?: string;
  tooltip?: string;
  isDisabled?: boolean;
  type?: string;
}

// 文本输入组件
export const TextInput: React.FC<TextInputProps> = ({
  label,
  name,
  value,
  onChange,
  error,
  placeholder,
  isRequired = false,
  helperText,
  tooltip,
  isDisabled = false,
  type = 'text',
}) => {
  return (
    <FormField
      label={label}
      name={name}
      error={error}
      isRequired={isRequired}
      helperText={helperText}
      tooltip={tooltip}
      isDisabled={isDisabled}
    >
      <Input
        id={name}
        name={name}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        type={type}
      />
    </FormField>
  );
};

// 数字输入属性
interface NumberFieldProps {
  label: string;
  name: string;
  value: string;
  onChange: (value: string) => void;
  error?: string;
  min?: number;
  max?: number;
  step?: number;
  isRequired?: boolean;
  helperText?: string;
  tooltip?: string;
  isDisabled?: boolean;
}

// 数字输入组件
export const NumberField: React.FC<NumberFieldProps> = ({
  label,
  name,
  value,
  onChange,
  error,
  min,
  max,
  step = 1,
  isRequired = false,
  helperText,
  tooltip,
  isDisabled = false,
}) => {
  return (
    <FormField
      label={label}
      name={name}
      error={error}
      isRequired={isRequired}
      helperText={helperText}
      tooltip={tooltip}
      isDisabled={isDisabled}
    >
      <NumberInput
        id={name}
        name={name}
        value={value}
        onChange={onChange}
        min={min}
        max={max}
        step={step}
      >
        <NumberInputField />
        <NumberInputStepper>
          <NumberIncrementStepper />
          <NumberDecrementStepper />
        </NumberInputStepper>
      </NumberInput>
    </FormField>
  );
};

// 选择框属性
interface SelectFieldProps {
  label: string;
  name: string;
  value: string;
  onChange: (value: string) => void;
  options: { value: string; label: string; disabled?: boolean }[];
  error?: string;
  isRequired?: boolean;
  helperText?: string;
  tooltip?: string;
  isDisabled?: boolean;
  placeholder?: string;
}

// 选择框组件
export const SelectField: React.FC<SelectFieldProps> = ({
  label,
  name,
  value,
  onChange,
  options,
  error,
  isRequired = false,
  helperText,
  tooltip,
  isDisabled = false,
  placeholder,
}) => {
  return (
    <FormField
      label={label}
      name={name}
      error={error}
      isRequired={isRequired}
      helperText={helperText}
      tooltip={tooltip}
      isDisabled={isDisabled}
    >
      <Select
        id={name}
        name={name}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
      >
        {options.map((option) => (
          <option
            key={option.value}
            value={option.value}
            disabled={option.disabled}
          >
            {option.label}
          </option>
        ))}
      </Select>
    </FormField>
  );
};
