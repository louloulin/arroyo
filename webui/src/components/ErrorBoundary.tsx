import React, { Component, ErrorInfo, ReactNode } from 'react';
import {
  Alert,
  AlertIcon,
  AlertTitle,
  AlertDescription,
  Box,
  Button,
  Code,
  Collapse,
} from '@chakra-ui/react';

interface Props {
  children: ReactNode;
  fallback?: ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
  errorInfo: ErrorInfo | null;
  showDetails: boolean;
}

class ErrorBoundary extends Component<Props, State> {
  public state: State = {
    hasError: false,
    error: null,
    errorInfo: null,
    showDetails: false,
  };

  public static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error, errorInfo: null, showDetails: false };
  }

  public componentDidCatch(error: Error, errorInfo: ErrorInfo) {
    this.setState({ error, errorInfo });
    console.error('Uncaught error:', error, errorInfo);
  }

  public render() {
    if (this.state.hasError) {
      if (this.props.fallback) {
        return this.props.fallback;
      }

      return (
        <Box p={4}>
          <Alert status="error" variant="solid" flexDirection="column" alignItems="start" mb={4}>
            <AlertIcon />
            <AlertTitle mt={0} mb={2}>
              Something went wrong
            </AlertTitle>
            <AlertDescription>
              {this.state.error?.message || 'An unexpected error occurred'}
            </AlertDescription>
            <Button
              size="sm"
              variant="outline"
              colorScheme="red"
              mt={2}
              onClick={() => this.setState({ showDetails: !this.state.showDetails })}
            >
              {this.state.showDetails ? 'Hide' : 'Show'} Details
            </Button>
          </Alert>

          <Collapse in={this.state.showDetails} animateOpacity>
            <Code p={4} borderRadius="md" width="100%" whiteSpace="pre-wrap">
              {this.state.error?.stack}
              {this.state.errorInfo?.componentStack}
            </Code>
          </Collapse>

          <Button
            mt={4}
            colorScheme="blue"
            onClick={() => {
              this.setState({ hasError: false, error: null, errorInfo: null });
              window.location.reload();
            }}
          >
            Reload Page
          </Button>
        </Box>
      );
    }

    return this.props.children;
  }
}

export default ErrorBoundary;
