import { Link, useNavigate, useParams } from 'react-router-dom';
import {
  Box,
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  Button,
  Container,
  Heading,
  Stack,
  Step,
  StepIcon,
  StepIndicator,
  StepNumber,
  StepSeparator,
  StepStatus,
  StepTitle,
  Stepper,
  useSteps,
} from '@chakra-ui/react';
import { ConnectionSchema, Connector, useConnectors } from '../../lib/data_fetching';
import { useEffect, useState } from 'react';

import { ConfigureConnection } from './ConfigureConnection';
import { DefineSchema } from './DefineSchema';
import { ConnectionTester } from './ConnectionTester';
import { ConfigureProfile } from './ConfigureProfile';
import { useNavbar } from '../../App';

import { PushTableConfig } from '../../types/push';
import { PushGlobalProvider } from '../../contexts/PushGlobalContext';

export type CreateConnectionState = {
  name: string | undefined;
  connectionProfileId: string | null;
  table: PushTableConfig | any; // 使用 PushTableConfig 类型，但也允许其他类型
  schema: ConnectionSchema | null;
};

export const ConnectionCreator = ({ connector }: { connector: Connector }) => {
  const [state, setState] = useState<CreateConnectionState>({
    name: undefined,
    connectionProfileId: null,
    table: null,
    schema: null,
  });

  // 计算步骤数量：
  // 1. 如果需要配置连接配置文件，加1步
  // 2. 配置表格总是需要的，加1步
  // 3. 如果需要自定义schema，加1步
  // 4. 最后的创建步骤，加1步
  let stepCount = 2; // 默认：配置表格 + 创建
  if (connector.connectionConfig || connector.id === 'push') {
    stepCount++; // 配置连接配置文件
  }
  if (connector.customSchemas) {
    stepCount++; // 自定义schema
  }

  const { activeStep, setActiveStep } = useSteps({
    index: 0,
    count: stepCount,
  });

  let steps = [];

  // 确保 Push Connector 始终有 Configure profile 步骤
  if (connector.connectionConfig || connector.id === 'push') {
    let next = steps.length + 1;
    steps.push({
      title: 'Configure profile',
      el: (
        <ConfigureProfile
          connector={connector}
          state={state}
          setState={setState}
          onSubmit={() => {
            console.log("ConfigureProfile onSubmit called, moving to step:", next);
            setActiveStep(next);
          }}
        />
      ),
    });
  }

  let next = steps.length + 1;
  steps.push({
    title: 'Configure table',
    el: (
      <ConfigureConnection
        connector={connector}
        state={state}
        setState={setState}
        onSubmit={() => {
          setActiveStep(next);
        }}
      />
    ),
  });

  if (connector.customSchemas) {
    let next = steps.length + 1;

    steps.push({
      title: 'Define schema',
      el: (
        <DefineSchema
          connector={connector}
          state={state}
          setState={setState}
          next={() => {
            setActiveStep(next);
          }}
        />
      ),
    });
  }
  steps.push({
    title: 'Create',
    el: <ConnectionTester connector={connector} state={state} setState={setState} />,
  });

  // 添加调试功能，允许手动跳转到下一步
  const handleNextStep = () => {
    if (activeStep < steps.length - 1) {
      setActiveStep(activeStep + 1);
    }
  };

  return (
    <PushGlobalProvider>
      <Stack spacing={8}>
        <Stepper index={activeStep}>
          {steps.map((step, index) => (
            <Step
              key={index}
              onClick={() => {
                if (activeStep > index) {
                  setActiveStep(index);
                }
              }}
            >
              <StepIndicator>
                <StepStatus
                  complete={<StepIcon />}
                  incomplete={<StepNumber />}
                  active={<StepNumber />}
                />
              </StepIndicator>

              <Box flexShrink="0">
                <StepTitle>{step.title}</StepTitle>
              </Box>

              <StepSeparator />
            </Step>
          ))}
        </Stepper>

        {steps[activeStep].el}
      </Stack>
    </PushGlobalProvider>
  );
};

export const CreateConnection = () => {
  let { connectorId } = useParams();
  let { connectors, connectorsLoading } = useConnectors();

  const { setMenuItems } = useNavbar();

  useEffect(() => {
    setMenuItems([]);
  }, []);

  let navigate = useNavigate();

  let connector = connectors?.find(c => c.id === connectorId);

  useEffect(() => {
    if (connectors != null && connector == null) {
      navigate('/connections/new');
    }
  });

  if (connectorsLoading || connector == null) {
    return <></>;
  } else {
    return (
      <Container py="8" flex="1">
        <Stack spacing={8} maxW={800}>
          <Breadcrumb>
            <BreadcrumbItem>
              <BreadcrumbLink as={Link} to="/connections">
                Connections
              </BreadcrumbLink>
            </BreadcrumbItem>
            <BreadcrumbItem>
              <BreadcrumbLink as={Link} to="/connections/new">
                Create Connection
              </BreadcrumbLink>
            </BreadcrumbItem>
            <BreadcrumbItem isCurrentPage>
              <BreadcrumbLink>{connector?.name}</BreadcrumbLink>
            </BreadcrumbItem>
          </Breadcrumb>

          <Stack spacing="4">
            <Heading size="sm">Create {connector?.name} connection</Heading>
          </Stack>

          <ConnectionCreator connector={connector!} />
        </Stack>
      </Container>
    );
  }
};
