Aqui está a documentação da estrutura do repositório, reescrita em um formato técnico e impessoal.

-----

# Documentação: Estrutura do Repositório Backend (AWS SAM)

## 1\. Visão Geral

Este documento descreve a organização e a estrutura de diretórios do repositório `mylib-backend`. O projeto é gerenciado como um monorepo e utiliza o **AWS Serverless Application Model (SAM)** para a definição e implantação da infraestrutura serverless (API Gateway, Lambda, DynamoDB) na AWS.

O repositório também contém um serviço desacoplado (`ml-server`) para o modelo de Machine Learning, que não é gerenciado pelo SAM.

## 2\. Estrutura de Diretórios

```bash
mylib-backend/
├── .aws-sam/              # Diretório de build do SAM (ignorado pelo git)
├── .gitignore
├── README.md              # Documentação principal do projeto
├── samconfig.toml         # Configurações de implantação do SAM
├── template.yaml          # Definição da stack (Infraestrutura como Código)
|
├── docs/
│   ├── backend.md    # Documentação do backend
│   ├── estrutura.md  # Documentação da estrutura
│   └── database.md   # Documentação do banco de dados
|
├── src/                     # Código-fonte principal
│   ├── functions/           # Funções Lambda
│   │       ├── catalog/      # Renomeado de 'commonLayer' para 'common_layer'
│   │       │     └── add_item.py 
│   │       └── system
│   │             ├── heartbeat.py
│   │             ├── search.py
│   │             └── requirementes.txt
│   └── layers/                # Código compartilhado (Lambda Layers)
│           └── common_layer/
│                   └── python/
│                         └── common/
│                               ├── __init__.py
│                               ├── decorators.py
│                               └── errors.py
|
└── ml_server/               # Serviço de ML (separado do SAM)
    ├── Dockerfile
    ├── requirements.txt
    └── src/
        ├── __init__.py
        └── main.py
```

-----

## 3\. Descrição dos Componentes

### 3.1. Arquivos Raiz (SAM)

  * **`template.yaml`**
    O arquivo de definição central da infraestrutura (IaC). Ele declara todos os recursos AWS gerenciados pelo SAM (Funções Lambda, API Gateway, Tabelas DynamoDB) e suas respectivas permissões (IAM Roles).
  * **`samconfig.toml`**
    Armazena configurações de implantação (deploy) para a CLI do SAM. Permite a definição de múltiplos perfis (ex: `[dev]`, `[prod]`) para parametrizar os deploys em diferentes ambientes, especificando S3 buckets, nomes de stack e parâmetros.

### 3.2. `src/functions/` (Funções Lambda)

Contém o código-fonte para as Funções Lambda. Cada subdiretório representa uma função independente, permitindo o gerenciamento isolado de dependências.

  * **`app.py`**: O módulo Python contendo a função *handler* (ex: `lambda_handler(event, context)`) que é invocada pelo AWS Lambda.
  * **`requirements.txt`**: Lista as dependências Python específicas de cada função. O SAM utiliza este arquivo para criar um pacote de implantação isolado (zip) durante o processo de `sam build`.

### 3.3. `src/layers/` (Lambda Layers)

Armazena código compartilhado (Layers) para ser reutilizado entre diferentes Funções Lambda, como clientes de banco de dados, lógica de formatação de resposta ou utilitários de log.

  * **Estrutura de Diretório `python/`**: A estrutura `python/` (ex: `layers/databaseClients/python/clients/`) é uma convenção exigida pelo AWS Lambda para que os pacotes sejam automaticamente adicionados ao `PYTHONPATH` do ambiente de execução.
  * **Importação**: Permite que as Funções Lambda importem o código do Layer diretamente (ex: `from clients.dynamo import DynamoClient`).
  * **Dependências do Layer**: O `requirements.txt` na raiz de um diretório de Layer (ex: `layers/databaseClients/requirements.txt`) instala dependências que o próprio código do Layer necessita.

### 3.4. `ml-server/` (Serviço de Recomendações)

Contém o código-fonte do serviço de recomendações (Machine Learning).

  * **Desacoplamento**: Este serviço **não é gerenciado ou implantado pelo AWS SAM**. Ele é tratado como um componente independente.
  * **Implantação**: É um serviço Python padrão (FastAPI) destinado à containerização (via `Dockerfile`) e implantação em uma plataforma de computação de longa duração (ex: AWS EC2, AWS Fargate, Render).
  * **Comunicação**: As Funções Lambda (especificamente `getRecommendations`) interagem com este serviço através de chamadas HTTP para seu endpoint exposto.