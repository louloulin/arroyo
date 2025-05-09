// 扩展API类型定义，添加PRQL支持相关的类型

import { components } from '../gen/api-types';

// 查询类型枚举
export enum QueryType {
  SQL = 'sql',
  PRQL = 'prql'
}

// 扩展ValidateQueryPost类型
export interface ExtendedValidateQueryPost {
  query: string;
  udfs?: { definition: string; language?: "python" | "rust" }[] | null;
  query_type?: QueryType;
}

// 扩展PipelinePost类型
export interface ExtendedPipelinePost {
  name: string;
  parallelism: number;
  query: string;
  udfs?: { definition: string; language?: "python" | "rust" }[] | null;
  query_type?: QueryType;
}

// 扩展PreviewPost类型
export interface ExtendedPreviewPost {
  query: string;
  udfs?: { definition: string; language?: "python" | "rust" }[] | null;
  enableSinks?: boolean;
  query_type?: QueryType;
}
