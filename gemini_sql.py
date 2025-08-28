"""
Custom text-to-SQL converter using Google Gemini 2.0 Flash directly
"""
import os
import json
from typing import Dict, Any, List
import google.generativeai as genai
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env.local'))

class GeminiTextToSQL:
    def __init__(self):
        """Initializing Gemini"""
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY environment variable is required")
        
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-2.0-flash-exp')
        
        # Training data storage
        self.schema_info = {}
        self.training_examples = []
        self.business_context = []
    
    def add_schema(self, schema_data: Dict[str, Any]):
        """Add database schema information"""
        self.schema_info = schema_data
    
    def add_training_example(self, question: str, sql: str, explanation: str = ""):
        """Add a training example"""
        self.training_examples.append({
            "question": question,
            "sql": sql,
            "explanation": explanation
        })
    
    def add_business_context(self, context: str):
        """Add business domain knowledge"""
        self.business_context.append(context)
    
    def _build_context_prompt(self) -> str:
        """Build the context prompt with schema and examples"""
        context_parts = []
        
        # Add schema information
        if self.schema_info:
            context_parts.append("DATABASE SCHEMA:")
            for table in self.schema_info.get('tables', []):
                table_name = table.get('name', '')
                columns = table.get('columns', [])
                if columns:
                    col_defs = ', '.join([f"{col['name']} {col['type']}" for col in columns])
                    context_parts.append(f"CREATE TABLE {table_name} ({col_defs});")
            context_parts.append("")
        
        # Add business context
        if self.business_context:
            context_parts.append("BUSINESS CONTEXT:")
            context_parts.extend(self.business_context)
            context_parts.append("")
        
        # Add training examples
        if self.training_examples:
            context_parts.append("EXAMPLE QUERIES:")
            for example in self.training_examples:
                context_parts.append(f"Question: {example['question']}")
                context_parts.append(f"SQL: {example['sql']}")
                if example['explanation']:
                    context_parts.append(f"Explanation: {example['explanation']}")
                context_parts.append("")
        
        return "\n".join(context_parts)
    
    def generate_sql(self, question: str) -> str:
        """Generate SQL from natural language question"""
        context = self._build_context_prompt()
        
        prompt = f"""You are an expert SQL query generator. Convert the natural language question to SQL.

{context}

RULES:
1. Generate only valid SQL queries
2. Use the exact table and column names from the schema
3. Return only the SQL query, no explanations
4. Use proper SQL syntax and best practices
5. If the question cannot be answered with the given schema, return "ERROR: Cannot generate SQL for this question"

Question: {question}

SQL Query:"""

        try:
            response = self.model.generate_content(prompt)
            sql = response.text.strip()
            
            # Clean up the response
            if sql.startswith("```sql"):
                sql = sql[6:]
            if sql.startswith("```"):
                sql = sql[3:]
            if sql.endswith("```"):
                sql = sql[:-3]
            
            return sql.strip()
        
        except Exception as e:
            return f"ERROR: Failed to generate SQL - {str(e)}"
    
    def explain_sql(self, sql: str) -> str:
        """Generate explanation for SQL query"""
        context = self._build_context_prompt()
        
        prompt = f"""Explain this SQL query in simple business terms.

{context}

SQL Query: {sql}

Explanation:"""

        try:
            response = self.model.generate_content(prompt)
            return response.text.strip()
        
        except Exception as e:
            return f"ERROR: Failed to explain SQL - {str(e)}"

# Global instance
gemini_sql = GeminiTextToSQL()

def train_gemini_sql():
    """Train the Gemini SQL generator with sample data"""
    print("🚀 Training Gemini Text-to-SQL...")
    
    # Add sample schema
    sample_schema = {
        "tables": [
            {
                "name": "customers",
                "columns": [
                    {"name": "id", "type": "TEXT PRIMARY KEY"},
                    {"name": "name", "type": "TEXT"},
                    {"name": "email", "type": "TEXT"},
                    {"name": "company", "type": "TEXT"},
                    {"name": "city", "type": "TEXT"},
                    {"name": "state", "type": "TEXT"},
                    {"name": "revenue", "type": "REAL"},
                    {"name": "created_at", "type": "TEXT"}
                ]
            },
            {
                "name": "orders",
                "columns": [
                    {"name": "id", "type": "TEXT PRIMARY KEY"},
                    {"name": "customer_id", "type": "TEXT"},
                    {"name": "product_name", "type": "TEXT"},
                    {"name": "amount", "type": "REAL"},
                    {"name": "status", "type": "TEXT"},
                    {"name": "order_date", "type": "TEXT"}
                ]
            }
        ]
    }
    
    gemini_sql.add_schema(sample_schema)
    
    # Add business context
    gemini_sql.add_business_context("Customers are small businesses using our AI platform")
    gemini_sql.add_business_context("Orders represent subscription payments or service purchases")
    gemini_sql.add_business_context("High-value customers have revenue > $10,000")
    gemini_sql.add_business_context("Status can be: 'pending', 'completed', 'cancelled', 'refunded'")
    gemini_sql.add_business_context("Revenue is monthly recurring revenue (MRR)")
    
    # Load training examples from JSON file
    examples_path = os.path.join(os.path.dirname(__file__), "gemini_training_examples.json")
    with open(examples_path, "r") as f:
        training_examples = json.load(f)
    for example in training_examples:
        gemini_sql.add_training_example(
            example["question"],
            example["sql"],
            example.get("explanation", "")
        )
    
    print("✅ Training completed!")
    return gemini_sql

if __name__ == "__main__":
    # Train the system
    trained_gemini = train_gemini_sql()
    
    # Test it
    print("\n🧪 Testing queries...")
    test_questions = [
        "Who are my top 5 customers?",
        "How many customers do we have?",
        "Show me recent orders",
        "What customers are from New York?"
    ]
    
    for question in test_questions:
        print(f"\nQ: {question}")
        sql = trained_gemini.generate_sql(question)
        print(f"SQL: {sql}")
        
        if not sql.startswith("ERROR"):
            explanation = trained_gemini.explain_sql(sql)
            print(f"Explanation: {explanation}")
