# Arroyo PRQL 高级功能实现

本文档详细介绍了 Arroyo PRQL 支持的高级功能实现，包括 Web UI 集成和性能优化。

## 1. Web UI 集成

为了提供更好的用户体验，我们在 Arroyo Web UI 中集成了 PRQL 支持，使用户能够轻松编写和管理 PRQL 查询。

### 1.1 PRQL 编辑器和语法高亮

我们为 Monaco 编辑器添加了 PRQL 语言支持，包括语法高亮、自动缩进和括号匹配等功能。

#### 1.1.1 语言定义

在 `webui/src/lib/prql-language.ts` 中，我们定义了 PRQL 语言的语法规则：

```typescript
export const prqlLanguageDefinition = {
  defaultToken: 'invalid',
  keywords: [
    'from', 'filter', 'select', 'derive', 'aggregate', 'sort', 'take', 'join', 'group', 'window',
    'let', 'prql', 'into', 'watermark', 'kafka', 'file', 'jdbc', 'http', 'as', 'tumbling', 'sliding',
    'session', 'field', 'delay', 'size', 'time_field', 'gap', 'slide', 'format', 'topic', 'path',
    'bootstrap.servers', 'write_mode', 'pattern', 'field_delimiter', 'quote_character', 'properties'
  ],
  // ... 其他语法规则
};
```

#### 1.1.2 编辑器配置

在 `webui/src/lib/monaco-setup.ts` 中，我们注册了 PRQL 语言：

```typescript
export function setupMonacoLanguages() {
  if (initialized) {
    return;
  }

  // Register PRQL language
  monaco.languages.register({ id: 'prql' });
  monaco.languages.setMonarchTokensProvider('prql', prqlLanguageDefinition);
  monaco.languages.setLanguageConfiguration('prql', prqlLanguageConfiguration);

  initialized = true;
}
```

#### 1.1.3 集成到编辑器组件

在 `webui/src/routes/pipelines/CodeEditor.tsx` 中，我们修改了编辑器组件以支持 PRQL：

```tsx
export function CodeEditor({
  code,
  setCode,
  readOnly,
  language,
}: {
  code: string;
  setCode?: Dispatch<string>;
  readOnly?: boolean;
  language?: string;
}) {
  // Setup Monaco languages when component mounts
  useEffect(() => {
    setupMonacoLanguages();
  }, []);

  // ... 其他代码
}
```

### 1.2 PRQL/SQL 切换功能

我们实现了 PRQL 和 SQL 之间的切换功能，使用户能够在两种语言之间自由切换。

#### 1.2.1 查询类型选择器

在 `webui/src/routes/pipelines/QueryTypeSelector.tsx` 中，我们创建了一个查询类型选择器组件：

```tsx
export function QueryTypeSelector({
  queryType,
  setQueryType,
  query,
  setQuery,
}: QueryTypeSelectorProps) {
  // ... 组件实现

  const handleTypeChange = async (newType: QueryType) => {
    if (newType === queryType) {
      return;
    }

    setConverting(true);
    try {
      if (newType === 'prql') {
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

  // ... 其他代码
}
```

#### 1.2.2 集成到管道编辑器

在 `webui/src/routes/pipelines/PipelineEditorTabs.tsx` 中，我们集成了查询类型选择器：

```tsx
<TabPanel height={'100%'} p={0} display={'flex'} flexDirection={'column'}>
  <Flex justifyContent="flex-end" p={2} bg="gray.800">
    <QueryTypeSelector
      queryType={queryType}
      setQueryType={setQueryType}
      query={queryInput}
      setQuery={updateQuery}
    />
  </Flex>
  <CodeEditor 
    code={queryInput} 
    setCode={updateQuery} 
    language={queryType}
  />
</TabPanel>
```

### 1.3 实时 SQL 预览

我们实现了实时 SQL 预览功能，使用户能够在编写 PRQL 查询的同时查看生成的 SQL 查询。

#### 1.3.1 转换 API

在 `crates/arroyo-api/src/prql.rs` 中，我们创建了一个 API 端点来处理 PRQL 到 SQL 的转换：

```rust
pub async fn convert_prql(
    State(state): State<AppState>,
    WithRejection(Json(request), _): WithRejection<Json<PrqlConvertRequest>, ApiError>,
) -> Result<Json<PrqlConvertResponse>, ErrorResp> {
    // Convert PRQL to SQL
    match arroyo_prql::prql_to_sql(&request.query) {
        Ok(sql) => Ok(Json(PrqlConvertResponse { sql })),
        Err(e) => Err(bad_request(format!("Failed to convert PRQL to SQL: {}", e))),
    }
}
```

#### 1.3.2 前端集成

在 `webui/src/lib/monaco-setup.ts` 中，我们实现了转换函数：

```typescript
export async function convertPrqlToSql(prql: string): Promise<string> {
  try {
    const response = await fetch('/api/v1/prql/convert', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ query: prql }),
    });

    if (!response.ok) {
      throw new Error('Failed to convert PRQL to SQL');
    }

    const data = await response.json();
    return data.sql;
  } catch (error) {
    console.error('Error converting PRQL to SQL:', error);
    // Return the original PRQL if conversion fails
    return prql;
  }
}
```

## 2. 性能优化

为了提高 PRQL 查询的处理性能，我们实现了多项优化措施。

### 2.1 查询缓存

我们实现了查询缓存，避免重复转换相同的查询，提高性能。

#### 2.1.1 缓存实现

在 `crates/arroyo-prql/src/lib.rs` 中，我们实现了一个全局缓存：

```rust
// Global cache for PRQL to SQL conversions
static PRQL_CACHE: Lazy<Arc<Mutex<HashMap<String, String>>>> = Lazy::new(|| {
    Arc::new(Mutex::new(HashMap::new()))
});
```

#### 2.1.2 缓存使用

在 `prql_to_sql` 函数中，我们使用缓存来避免重复转换：

```rust
pub fn prql_to_sql(prql_query: &str) -> Result<String, PrqlError> {
    // Check cache first
    if let Some(cached_sql) = get_from_cache(prql_query) {
        return Ok(cached_sql);
    }

    // ... 转换逻辑

    // Add to cache
    add_to_cache(prql_query, &sql);

    Ok(sql)
}
```

#### 2.1.3 缓存管理

我们实现了缓存管理函数，包括获取和添加缓存：

```rust
fn get_from_cache(prql_query: &str) -> Option<String> {
    let cache = PRQL_CACHE.lock().ok()?;
    cache.get(prql_query).cloned()
}

fn add_to_cache(prql_query: &str, sql: &str) {
    if let Ok(mut cache) = PRQL_CACHE.lock() {
        // Limit cache size to 1000 entries
        if cache.len() >= 1000 {
            // Simple strategy: clear the cache when it gets too big
            cache.clear();
        }
        cache.insert(prql_query.to_string(), sql.to_string());
    }
}
```

### 2.2 解析优化

我们优化了 PRQL 解析过程，提高了解析速度和准确性。

#### 2.2.1 语法解析优化

我们优化了扩展语法的解析逻辑，使其更高效：

```rust
pub fn parse_extended_syntax(prql_query: &str) -> Result<(String, Vec<ConnectorConfig>, Vec<WindowConfig>, Vec<WatermarkConfig>), PrqlError> {
    // ... 优化的解析逻辑
}
```

#### 2.2.2 错误处理优化

我们改进了错误处理，提供更详细的错误信息：

```rust
pub enum PrqlError {
    ParseError(String),
    CompilationError(String),
    ConversionError(String),
}
```

### 2.3 SQL 生成优化

我们优化了 SQL 生成过程，生成更高效的 SQL 查询。

#### 2.3.1 SQL 后处理优化

我们优化了 SQL 后处理逻辑，使生成的 SQL 更高效：

```rust
pub fn post_process_sql(sql: &str) -> Result<String, PrqlError> {
    // ... 优化的后处理逻辑
}
```

## 3. 未来改进

虽然我们已经实现了许多高级功能，但仍有一些改进可以在未来实现：

### 3.1 Web UI 改进

- 添加 PRQL 代码片段库，提供常用查询模板
- 实现 PRQL 查询验证和错误提示
- 添加 PRQL 自动完成功能

### 3.2 性能改进

- 实现更高级的缓存策略，如 LRU 缓存
- 优化解析算法，提高大型查询的处理速度
- 实现并行处理，利用多核 CPU 提高性能

### 3.3 功能扩展

- 支持更多 PRQL 高级功能，如自定义函数和模块
- 添加更多 Arroyo 特定功能的支持
- 实现 PRQL 查询优化器，自动优化查询性能
