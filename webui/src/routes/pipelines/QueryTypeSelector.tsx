import React, { useState } from 'react';
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
import { convertPrqlToSql, convertSqlToPrql, isPrqlQuery } from '../../lib/monaco-setup';
import { QueryType } from '../../lib/api-types-extended';

interface QueryTypeSelectorProps {
  queryType: QueryType;
  setQueryType: (type: QueryType) => void;
  query: string;
  setQuery: (query: string) => void;
}

export function QueryTypeSelector({
  queryType,
  setQueryType,
  query,
  setQuery,
}: QueryTypeSelectorProps) {
  const { isOpen, onOpen, onClose } = useDisclosure();
  const [converting, setConverting] = useState(false);

  const handleTypeChange = async (newType: QueryType) => {
    if (newType === queryType) {
      return;
    }

    setConverting(true);
    try {
      if (newType === QueryType.PRQL) {
        // Convert SQL to PRQL
        const prql = await convertSqlToPrql(query);
        setQuery(prql);
      } else {
        // Convert PRQL to SQL
        const sql = await convertPrqlToSql(query);
        setQuery(sql);
      }
      setQueryType(newType);
    } catch (error) {
      console.error(`Error converting ${queryType} to ${newType}:`, error);
    } finally {
      setConverting(false);
      onClose();
    }
  };

  const autoDetectType = async () => {
    const detectedType = isPrqlQuery(query) ? QueryType.PRQL : QueryType.SQL;
    if (detectedType !== queryType) {
      setQueryType(detectedType);
    }
  };

  return (
    <Flex alignItems="center">
      <Tooltip label={queryType === QueryType.SQL ? 'SQL Query' : 'PRQL Query'}>
        <Button
          size="xs"
          variant="ghost"
          leftIcon={queryType === QueryType.SQL ? <Icon as={PiFileSqlDuotone} /> : <Icon as={FiCode} />}
          onClick={autoDetectType}
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
                isDisabled={converting || queryType === QueryType.SQL}
                onClick={() => handleTypeChange(QueryType.SQL)}
                colorScheme={queryType === QueryType.SQL ? 'blue' : 'gray'}
                leftIcon={<Icon as={PiFileSqlDuotone} />}
              >
                SQL
              </Button>
              <Button
                isDisabled={converting || queryType === QueryType.PRQL}
                onClick={() => handleTypeChange(QueryType.PRQL)}
                colorScheme={queryType === QueryType.PRQL ? 'blue' : 'gray'}
                leftIcon={<Icon as={FiCode} />}
              >
                PRQL
              </Button>
            </ButtonGroup>
            {converting && <Text mt={2}>Converting...</Text>}
          </PopoverBody>
        </PopoverContent>
      </Popover>
    </Flex>
  );
}
