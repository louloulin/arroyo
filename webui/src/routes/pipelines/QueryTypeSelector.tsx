import {
  Button,
  ButtonGroup,
  Flex,
  Icon,
  IconButton,
  Popover,
  PopoverArrow,
  PopoverBody,
  PopoverContent,
  PopoverTrigger,
  Text,
  Tooltip,
  useDisclosure,
} from '@chakra-ui/react';
import { PiFileSqlDuotone } from 'react-icons/pi';
import { FiCode } from 'react-icons/fi';
import { MdSwapHoriz } from 'react-icons/md';
import { isPrqlQuery } from '../../lib/monaco-setup';
import { QueryType } from '../../lib/api-types-extended';

interface QueryTypeSelectorProps {
  queryType: QueryType;
  setQueryType: (type: QueryType) => void;
  query: string;
}

export function QueryTypeSelector({
  queryType,
  setQueryType,
  query,
}: QueryTypeSelectorProps) {
  const { isOpen, onOpen, onClose } = useDisclosure();

  const handleTypeChange = async (newType: QueryType) => {
    if (newType === queryType) {
      return;
    }

    // 只切换类型，不进行转换
    setQueryType(newType);
    onClose();
  };

  const toggleQueryType = async () => {
    // 直接切换到另一种查询类型
    const newType = queryType === QueryType.SQL ? QueryType.PRQL : QueryType.SQL;
    setQueryType(newType);
  };

  return (
    <Flex alignItems="center">
      <Tooltip label={queryType === QueryType.SQL ? 'SQL Query' : 'PRQL Query'}>
        <Button
          size="xs"
          variant="ghost"
          leftIcon={queryType === QueryType.SQL ? <Icon as={PiFileSqlDuotone} /> : <Icon as={FiCode} />}
          onClick={toggleQueryType}
        >
          {queryType === QueryType.SQL ? 'SQL' : 'PRQL'}
        </Button>
      </Tooltip>

      <Popover isOpen={isOpen} onOpen={onOpen} onClose={onClose} placement="bottom-start">
        <PopoverTrigger>
          <IconButton
            aria-label="Switch query type"
            icon={<MdSwapHoriz />}
            size="xs"
            variant="ghost"
          />
        </PopoverTrigger>
        <PopoverContent width="auto">
          <PopoverArrow />
          <PopoverBody>
            <Text mb={2}>Switch query language:</Text>
            <ButtonGroup size="sm" isAttached variant="outline">
              <Button
                isDisabled={queryType === QueryType.SQL}
                onClick={() => handleTypeChange(QueryType.SQL)}
                colorScheme={queryType === QueryType.SQL ? 'blue' : 'gray'}
                leftIcon={<Icon as={PiFileSqlDuotone} />}
              >
                SQL
              </Button>
              <Button
                isDisabled={queryType === QueryType.PRQL}
                onClick={() => handleTypeChange(QueryType.PRQL)}
                colorScheme={queryType === QueryType.PRQL ? 'blue' : 'gray'}
                leftIcon={<Icon as={FiCode} />}
              >
                PRQL
              </Button>
            </ButtonGroup>
          </PopoverBody>
        </PopoverContent>
      </Popover>
    </Flex>
  );
}
