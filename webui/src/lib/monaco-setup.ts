import * as monaco from 'monaco-editor';
import { prqlLanguageConfiguration, prqlLanguageDefinition } from './prql-language';
import { languages } from 'monaco-editor';

let initialized = false;

export function setupMonacoLanguages() {
  if (initialized) {
    return;
  }

  // Register PRQL language
  monaco.languages.register({ id: 'prql' });
  monaco.languages.setMonarchTokensProvider('prql', prqlLanguageDefinition as any);
  monaco.languages.setLanguageConfiguration('prql', prqlLanguageConfiguration as any);

  initialized = true;
}

// Call this function to convert SQL to PRQL
export async function convertSqlToPrql(sql: string): Promise<string> {
  // This is a placeholder. In a real implementation, you would call your API
  // to convert SQL to PRQL. For now, we'll just return a simple transformation.
  return sql
    .replace(/SELECT\s+(.*?)\s+FROM\s+(\w+)/i, 'from $2\nselect {$1}')
    .replace(/WHERE\s+(.*)/i, 'filter $1');
}

// Call this function to convert PRQL to SQL
export async function convertPrqlToSql(prql: string): Promise<string> {
  // This is a placeholder. In a real implementation, you would call your API
  // to convert PRQL to SQL. For now, we'll just return a simple transformation.
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

// Detect if a query is PRQL
export function isPrqlQuery(query: string): boolean {
  const query_trimmed = query.trim();

  // Check for common PRQL starting keywords
  if (query_trimmed.startsWith('from') || query_trimmed.startsWith('let') || query_trimmed.startsWith('prql')) {
    return true;
  }

  // Check for pipe operator usage
  if (query_trimmed.includes('|') && !query_trimmed.includes('SELECT') && !query_trimmed.includes('FROM')) {
    return true;
  }

  // Check for PRQL-style function calls (no parentheses)
  if (query_trimmed.includes('filter ') || query_trimmed.includes('derive ') || query_trimmed.includes('group ')) {
    return true;
  }

  return false;
}
