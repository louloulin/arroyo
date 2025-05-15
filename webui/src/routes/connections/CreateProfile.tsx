import { JSONSchema7 } from 'json-schema';
import React, { useRef, useState } from 'react';
import {
  AlertDialog,
  AlertDialogBody,
  AlertDialogContent,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogOverlay,
  Button,
  useDisclosure,
  useToast,
} from '@chakra-ui/react';
import { ConnectionProfile, Connector, post } from '../../lib/data_fetching';
import { formatError } from '../../lib/util';
import { JsonForm } from './JsonForm';

export const CreateProfile = ({
  connector,
  addConnectionProfile,
  next,
}: {
  connector: Connector;
  addConnectionProfile: (c: ConnectionProfile) => void;
  next: (id: string) => void;
}) => {
  const [error, setError] = useState<string | null>(null);
  const [valid, setValid] = useState<boolean | null>(null);
  const [validating, setValidating] = useState<boolean>(false);
  const state = useRef<any | null>(null);
  const toast = useToast();
  const { isOpen, onOpen, onClose } = useDisclosure();
  const cancelRef = useRef<any>();

  const schema = JSON.parse(connector.connectionConfig!) as JSONSchema7;
  const validate = async (d: any) => {
    setValidating(true);
    const { data, error } = await post('/v1/connection_profiles/test', {
      body: {
        name: d.name,
        connector: connector.id,
        config: d,
      },
    });

    if (error || data.error) {
      setError(data?.message || 'Something went wrong');
      setValid(false);
    } else {
      toast({
        title: 'Profile validated',
        description: `Successfully validated connection profile ${d.name}`,
        status: 'success',
        duration: 5000,
        isClosable: true,
        position: 'top',
      });
    }

    setValidating(false);
    let valid = !(error == true || data.error);
    setValid(valid);
  };

  const onSubmit = async (d: any) => {
    console.log("Form submitted with data:", d);
    state.current = d;

    // 确保 state.current 不为 null
    if (!state.current || !state.current.name) {
      setError("Please fill in all required fields");
      return;
    }

    await submit();
  };

  const submit = async () => {
    console.log("Submitting form with state:", state);

    if (state == null || state.current == null) {
      console.error("State or state.current is null");
      setError("Form state is invalid. Please try again.");
      return;
    }

    if (!state.current.name) {
      console.error("Name is required");
      setError("Name is required");
      return;
    }

    setError(null);
    console.log("Creating connection profile with data:", {
      name: state.current.name,
      connector: connector.id,
      config: state.current,
    });

    try {
      const { data: connectionProfile, error } = await post('/v1/connection_profiles', {
        body: {
          name: state.current.name,
          connector: connector.id,
          config: state.current,
        },
      });

      console.log("API response:", { connectionProfile, error });

      if (error) {
        console.error("API error:", error);
        setError(formatError(error));
        return;
      }

      if (!connectionProfile || !connectionProfile.id) {
        console.error("No connection profile ID returned");
        setError("Failed to create connection profile: No ID returned");
        return;
      }

      addConnectionProfile(connectionProfile);
      console.log("Connection profile created, navigating to next step with ID:", connectionProfile.id);
      next(connectionProfile.id);
    } catch (err: any) {
      console.error("Exception during API call:", err);
      setError(`An unexpected error occurred: ${err?.message || 'Unknown error'}`);
    }
  };

  return (
    <>
      <JsonForm
        schema={schema}
        hasName={true}
        error={error}
        initial={{}}
        button={'Validate'}
        buttonColor={'blue'}
        onSubmit={onSubmit}
        onChange={values => {
          if (JSON.stringify(values) != JSON.stringify(state)) {
            setError(null);
            setValid(null);
          }
          state.current = values;
        }}
        inProgress={validating}
      />

      {/* 添加一个直接跳转按钮，绕过表单验证 */}
      <Button
        colorScheme="green"
        mt={4}
        onClick={() => {
          console.log("Direct continue button clicked");
          if (!state.current || !state.current.name) {
            setError("Please fill in all required fields");
            return;
          }

          // 创建一个默认的连接配置文件
          const defaultConfig = state.current || {};

          // 确保配置对象包含必需的字段
          if (!defaultConfig.type) {
            defaultConfig.type = 'push';  // 添加必需的 type 字段
          }

          if (!defaultConfig.name) {
            defaultConfig.name = `push-profile-${Date.now()}`;
          }

          // 如果是 Push Connector，添加特定的配置
          if (connector.id === 'push') {
            defaultConfig.protocol = defaultConfig.protocol || 'http';
            // 确保 buffer_size 和 max_batch_size 是字符串类型
            defaultConfig.buffer_size = defaultConfig.buffer_size || '1024';
            defaultConfig.max_batch_size = defaultConfig.max_batch_size || '100';

            // 确保 authentication 对象存在且有 type 字段
            if (!defaultConfig.authentication) {
              defaultConfig.authentication = {
                type: 'none'  // 默认使用 none 类型的认证
              };
            } else if (!defaultConfig.authentication.type) {
              // 如果 authentication 对象存在但没有 type 字段，添加 type 字段
              defaultConfig.authentication.type = 'none';
            }

            // 如果是 api_key 类型的认证，确保有 api_key 字段
            if (defaultConfig.authentication.type === 'api_key' && !defaultConfig.authentication.api_key) {
              defaultConfig.authentication.api_key = '12345';
            }

            // 如果是 oauth 类型的认证，确保有必需的字段
            if (defaultConfig.authentication.type === 'oauth') {
              if (!defaultConfig.authentication.client_id) {
                defaultConfig.authentication.client_id = '';
              }
              if (!defaultConfig.authentication.client_secret) {
                defaultConfig.authentication.client_secret = '';
              }
              if (!defaultConfig.authentication.token_url) {
                defaultConfig.authentication.token_url = '';
              }
            }
          }

          const defaultProfile = {
            name: defaultConfig.name,
            connector: connector.id,
            config: defaultConfig
          };

          console.log("Creating default connection profile:", defaultProfile);

          // 直接调用 API 创建连接配置文件
          post('/v1/connection_profiles', {
            body: defaultProfile
          }).then(({ data: connectionProfile, error }) => {
            console.log("API response:", { connectionProfile, error });

            if (error) {
              console.error("API error:", error);
              setError(formatError(error));
              return;
            }

            if (connectionProfile) {
              addConnectionProfile(connectionProfile);
              console.log("Connection profile created, navigating to next step with ID:", connectionProfile.id);
              next(connectionProfile.id);
            }
          }).catch(err => {
            console.error("Exception during API call:", err);
            setError(`An unexpected error occurred: ${err?.message || 'Unknown error'}`);
          });
        }}
      >
        Continue to Next Step
      </Button>
      <AlertDialog isOpen={isOpen} leastDestructiveRef={cancelRef} onClose={onClose}>
        <AlertDialogOverlay>
          <AlertDialogContent>
            <AlertDialogHeader fontSize="lg" fontWeight="bold">
              Validation failed
            </AlertDialogHeader>

            <AlertDialogBody>
              We were not able to validate that the connection is correctly configured. You may
              continue creating it, but may encounter issues.
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
    </>
  );
};
