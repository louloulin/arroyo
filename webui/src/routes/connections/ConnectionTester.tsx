import { Dispatch, useRef, useState } from 'react';
import { CreateConnectionState } from './CreateConnection';
import {
  Alert,
  AlertDescription,
  AlertDialog,
  AlertDialogBody,
  AlertDialogContent,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogOverlay,
  AlertIcon,
  AlertTitle,
  Box,
  Button,
  FormControl,
  FormHelperText,
  FormLabel,
  Input,
  Spinner,
  Stack,
  StackDivider,
  Text,
  useDisclosure,
  useToast,
} from '@chakra-ui/react';
import { useNavigate } from 'react-router-dom';
import {
  ConnectionTablePost,
  Connector,
  post,
  TestSourceMessage,
  useConnectionTableTest,
} from '../../lib/data_fetching';
import { formatError } from '../../lib/util';
import { useError, ErrorDisplay } from '../../contexts/ErrorContext';
import { validatePushTableConfig } from '../../utils/validation';
import { PushTableConfig } from '../../types/push';

export function ConnectionTester({
  connector,
  state,
  setState,
}: {
  connector: Connector;
  state: CreateConnectionState;
  setState: Dispatch<CreateConnectionState>;
}) {
  const [testing, setTesting] = useState<boolean>(false);
  const [messages, setMessages] = useState<Array<TestSourceMessage>>([]);
  const { isOpen, onOpen, onClose } = useDisclosure();
  const cancelRef = useRef<any>();
  const [touched, setTouched] = useState<boolean>(false);
  const [localError, setLocalError] = useState<{ title: string; body: string } | null>(null);
  const navigate = useNavigate();
  const { addValidationError, addServerError, clearError } = useError();
  const toast = useToast();

  const done = messages.length > 0 && messages[messages?.length - 1].done;
  const errored = messages.find(m => m.error) != null;

  let config = state.table ? JSON.parse(JSON.stringify(state.table)) : {};
  if (config && config.__meta) {
    delete config.__meta;
  }

  const isValidSQLTableName = (name: string | undefined) => {
    // This is a very basic check and may not cover all cases.
    // Update this according to your actual validation rules.
    return name && /^[_a-zA-Z][a-zA-Z0-9_]*$/.test(name);
  };

  // 确保 connectionProfileId 不为 null，特别是对于 Push Connector
  let connectionProfileId = state.connectionProfileId;
  if (!connectionProfileId && connector.id === 'push') {
    console.error('Push Connector requires a connection profile, but none was specified');
  }

  const createRequest: ConnectionTablePost = {
    name: state.name!,
    connector: connector.id,
    connectionProfileId: connectionProfileId,
    config: config,
    schema: state.schema || undefined,
  };

  const onClickTest = async () => {
    if (!testing) {
      clearError();

      // 检查是否有 connectionProfileId，特别是对于 Push Connector
      if (!connectionProfileId && connector.id === 'push') {
        addValidationError('Connection profile required', [
          'This connector requires a connection profile, but none was specified.',
          'Please go back to the first step and select or create a connection profile.'
        ]);
        return;
      }

      // 验证表单
      if (connector.id === 'push') {
        const validation = validatePushTableConfig(state.table as PushTableConfig);
        if (!validation.valid) {
          addValidationError('Please fix the following errors:', validation.errors);
          return;
        }
      }

      setTesting(true);
      setLocalError(null);

      let messages: Array<TestSourceMessage> = [];
      setMessages(messages);

      try {
        await useConnectionTableTest(event => {
          messages = [...messages, event];
          setMessages(messages);

          // 如果有错误，显示错误消息
          if (event.error) {
            addServerError('Connection test failed', [event.message]);
          }

          // 如果测试完成且成功，显示成功消息
          if (event.done && !event.error) {
            toast({
              title: 'Connection test successful',
              description: 'Your connection has been successfully tested.',
              status: 'success',
              duration: 3000,
              isClosable: true,
            });
          }
        }, createRequest);
      } catch (error) {
        console.error('Connection test error:', error);
        addServerError('Connection test failed', [error instanceof Error ? error.message : String(error)]);
      } finally {
        setTesting(false);
      }
    }
  };

  const submit = async () => {
    clearError();

    // 检查是否有 connectionProfileId，特别是对于 Push Connector
    if (!connectionProfileId && connector.id === 'push') {
      addValidationError('Connection profile required', [
        'This connector requires a connection profile, but none was specified.',
        'Please go back to the first step and select or create a connection profile.'
      ]);
      return;
    }

    // 验证表单
    if (connector.id === 'push') {
      const validation = validatePushTableConfig(state.table as PushTableConfig);
      if (!validation.valid) {
        addValidationError('Please fix the following errors:', validation.errors);
        return;
      }
    }

    setLocalError(null);

    try {
      const { error } = await post('/v1/connection_tables', { body: createRequest });
      if (error) {
        addServerError('Failed to create connection', [formatError(error)]);
      } else {
        toast({
          title: 'Connection created',
          description: 'Your connection has been successfully created.',
          status: 'success',
          duration: 3000,
          isClosable: true,
        });
        navigate('/connections');
      }
    } catch (error) {
      console.error('Connection creation error:', error);
      addServerError('Failed to create connection', [error instanceof Error ? error.message : String(error)]);
    }
  };

  const onClickContinue = async () => {
    if (errored) {
      onOpen();
    } else {
      submit();
    }
  };

  let messageBox = null;
  if (messages.length) {
    messageBox = (
      <Box bg="bg-surface">
        <Stack divider={<StackDivider />} spacing="0">
          {messages.map((m, i) => {
            let status: 'error' | 'success' | 'info' = m.error
              ? 'error'
              : m.done
              ? 'success'
              : 'info';
            return (
              <Alert key={i} status={status}>
                <AlertIcon />
                <AlertDescription>{m.message}</AlertDescription>
              </Alert>
            );
          })}
        </Stack>
      </Box>
    );
  }

  return (
    <>
      <ErrorDisplay />

      {localError && (
        <Alert status="error" mb={4}>
          <AlertIcon />
          <AlertTitle>{localError.title}</AlertTitle>
          <AlertDescription>{localError.body}</AlertDescription>
        </Alert>
      )}

      <Stack spacing={8} maxW={'md'}>
        <FormControl isInvalid={touched && (state.name === '' || !isValidSQLTableName(state.name))}>
          <FormLabel>Connection Name</FormLabel>
          <Input
            type="text"
            value={state.name || ''}
            onChange={v => {
              setTouched(true);
              setState({ ...state, name: v.target.value });
            }}
          />
          <FormHelperText>
            The connection will used in SQL using this name; it must be a {'\u00A0'}
            <dfn title="Names must start with a letter or _, contain only letters, numbers, and _s, and have fewer than 63 characters">
              valid SQL table name
            </dfn>
            <Text
              mt={2}
              style={{
                color:
                  touched && (!state.name || !isValidSQLTableName(state.name)) ? 'red' : 'inherit',
              }}
            >
              {touched && state.name === '' && "Table Name can't be empty."}
              {touched &&
                state.name !== '' &&
                !isValidSQLTableName(state.name) &&
                'Table Name is not a valid SQL table name.'}
            </Text>
          </FormHelperText>
        </FormControl>

        <Text>Before creating the connection, we can validate that it is configured properly.</Text>

        <Button
          colorScheme="blue"
          isDisabled={testing || state.name === '' || !isValidSQLTableName(state.name)}
          onClick={onClickTest}
        >
          Test Connection
        </Button>

        {messageBox}

        {testing && !done && (
          <Box>
            <Spinner />
          </Box>
        )}

        {done && (
          <Button colorScheme={errored ? 'red' : 'green'} onClick={onClickContinue}>
            Create
          </Button>
        )}

        {!done && !testing && state.name && isValidSQLTableName(state.name) && (
          <Button colorScheme="green" mt={4} onClick={submit}>
            Skip Test and Create
          </Button>
        )}

        <AlertDialog isOpen={isOpen} leastDestructiveRef={cancelRef} onClose={onClose}>
          <AlertDialogOverlay>
            <AlertDialogContent>
              <AlertDialogHeader fontSize="lg" fontWeight="bold">
                Validation failed
              </AlertDialogHeader>

              <AlertDialogBody>
                We were not able to validate that the connection is correctly configured. You may
                continue creating it, but may encounter issues when using it in a query.
              </AlertDialogBody>

              <AlertDialogFooter>
                <Button ref={cancelRef} onClick={onClose}>
                  Go Back
                </Button>
                <Button
                  colorScheme="red"
                  onClick={() => {
                    onClose();
                    submit();
                  }}
                  ml={3}
                >
                  Create
                </Button>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialogOverlay>
        </AlertDialog>
      </Stack>
    </>
  );
}
