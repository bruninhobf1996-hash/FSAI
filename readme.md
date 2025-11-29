# FSAI — Assistente Inteligente para Consultas em Data Warehouse

Este projeto foi desenvolvido por **Bruno Barbosa** e tem como objetivo permitir que usuários realizem perguntas em linguagem natural e recebam respostas automáticas baseadas em dados armazenados em um banco MySQL.

O sistema utiliza técnicas de IA generativa e RAG (Retrieval-Augmented Generation) para:

- Identificar quais tabelas e colunas são relevantes para a pergunta do usuário  
- Gerar automaticamente uma consulta SQL segura  
- Executar a consulta no banco de dados  
- Transformar os resultados em uma resposta clara e objetiva  

---

## 🧠 Tecnologias Utilizadas

- **Python**
- **FastAPI**
- **MySQL**
- **OpenAI GPT**
- **Embeddings (text-embedding-3-small)**
- **Uvicorn**
- **YAML**

---

## 🚀 Como Executar o Projeto

1. Instale as dependências:
```bash
pip install -r requirements.txt

Crie um arquivo .env na raiz do projeto com:
OPENAI_API_KEY=sua_chave
MYSQL_HOST=45.33.0.225
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=sua_senha
MYSQL_DATABASE=seu_banco
GEN_MODEL=gpt-4o-mini
EMBED_MODEL=text-embedding-3-small

2. Crie um arquivo .env na raiz do projeto com:
OPENAI_API_KEY=sua_chave
MYSQL_HOST=45.33.0.225
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=sua_senha
MYSQL_DATABASE=seu_banco
GEN_MODEL=gpt-4o-mini
EMBED_MODEL=text-embedding-3-small

3.Inicie o servidor:
uvicorn main:app --reload

Exemplo de Requisição
{
  "user_id": "1",
  "department": "financeiro",
  "prompt": "Quais foram as vendas do último mês?",
  "lang": "pt-BR"
}

Estrutura Simplificada do Projeto
FSAI/
│── main.py
│── schema.yaml
│── requirements.txt
│── README.md




