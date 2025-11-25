# MyLib — Arquitetura de Backend

Esta documentação descreve a arquitetura de backend do MyLib, que utiliza uma abordagem híbrida combinando serviços **AWS Serverless** para a API principal e um **servidor dedicado** para processamento de Machine Learning (Recomendações).

## 🏛️ Visão Geral da Arquitetura

O sistema é dividido em duas partes principais:

1.  **API Core (Serverless):** O "cérebro" principal da aplicação, responsável por todo o CRUD (Create, Read, Update, Delete) de dados do usuário, autenticação e lógica de negócios padrão. É construído com **API Gateway** e **AWS Lambda**.
2.  **Serviço de Recomendações (Servidor Dedicado):** Um microserviço desacoplado e especializado, rodando em um ambiente de longa duração (como **EC2** ou **Render**), responsável por executar modelos de ML e gerar recomendações.

-----

## 📦 Componentes Principais

### 1\. AWS API Gateway

É o "portão de entrada" (front-door) para toda a nossa API.

  * **Função:** Recebe todas as requisições HTTP dos clientes (web/mobile).
  * **Roteamento:** Encaminha cada rota (ex: `POST /item`, `GET /profile`) para o Lambda correto.
  * **Autenticação:** Integrado com o AWS Cognito para validar os tokens JWT do usuário em cada requisição protegida.
  * **Segurança:** Gerencia throttling (limitação de requisições), CORS e validação de requests.

### 2\. AWS Lambda

Onde nossa lógica de negócios reside.

  * **Função:** Executa o código da aplicação em resposta a eventos do API Gateway.
  * **Modelo:** Funções curtas e "stateless". Cada função lida com uma responsabilidade específica (ex: `updateProfile`, `addItemToList`, `getRecommendations`).
  * **Interação:**
      * Lê e escreve dados do usuário no **DynamoDB**.
      * **Orquestra** o fluxo de recomendações: quando o `getRecommendationsLambda` é chamado, ele **envia uma requisição HTTP** para o Serviço de Recomendações.

### 3\. AWS DynamoDB

Nosso banco de dados de usuário (User Data Layer).

  * **Função:** Armazena todos os dados gerados pelo usuário (perfis, avaliações, listas, status), conforme a [Arquitetura de Dados](database.md).
  * **Acesso:** Acessado exclusivamente pelos Lambdas.

### 4\. Supabase

Nosso catálogo de mídias (Content Dataset Layer).

  * **Função:** Armazena os metadados globais e estáticos de todas as mídias (filmes, jogos, etc.).
  * **Acesso:**
      * **Acesso Primário (Lambdas):** Quando um Lambda precisa "hidratar" uma resposta (ex: retornar os detalhes de um filme junto com a nota do usuário), ele consulta o Supabase.
      * **Acesso Secundário (Cliente):** O cliente também pode ter credenciais (read-only) para buscar dados públicos do Supabase diretamente (ex: em uma tela de "explorar"), aliviando a carga dos Lambdas.

-----

## 🧠 Serviço de Recomendações (ML Service)

Este é um componente crítico e desacoplado da API principal.

  * **Plataforma:** Servidor dedicado (ex: **AWS EC2**, **Render**, ou outro provedor de PaaS).
  * **Propósito:** Lidar com tarefas computacionalmente intensivas ou de longa duração que não são adequadas para o ambiente Lambda (que tem limites de tempo e memória).
  * **Interface:** Expõe sua própria API REST interna (ex: `POST /v1/recommendations`). Esta API **não** deve ser exposta publicamente, sendo acessível apenas pelos nossos Lambdas (idealmente via VPC ou chaves de API internas).
  * **Funcionamento:** Mantém o modelo de ML carregado na memória, pronto para receber requisições, processar os dados do usuário e retornar uma lista de IDs de mídia recomendados.

-----

## 🌊 Fluxos de Trabalho Essenciais

### Fluxo 1: Adicionar Mídia (Operação Serverless Padrão)

1.  O **Cliente** envia um `POST /item` com `(media_id: "001244", rating: 8)` e o Token JWT.
2.  O **API Gateway** recebe, valida o Token JWT (via Cognito) e aciona o Lambda `AddItemLambda`.
3.  O **`AddItemLambda`** executa a lógica.
4.  O Lambda grava o item no **DynamoDB** (PK: `user_id`, SK: `item#anime#001244`, `rating: 8`).
5.  O Lambda retorna `Status 201 (Created)` para o Cliente.

### Fluxo 2: Pedir Recomendações (Operação Híbrida)

1.  O **Cliente** envia um `GET /recommendations` com o Token JWT.
2.  O **API Gateway** recebe, valida o token e aciona o Lambda `GetRecommendationsLambda`.
3.  O **`GetRecommendationsLambda`** consulta o **DynamoDB** para buscar o histórico de mídias do usuário.
4.  O Lambda **envia uma requisição HTTP** (ex: `POST`) para o endpoint interno do **Serviço de Recomendações**, passando o histórico do usuário no *body*.
5.  O **Serviço de Recomendações** (EC2/Render) recebe a requisição, processa os dados no modelo de ML e retorna uma lista de IDs (ex: `[987321, 550]`).
6.  O **`GetRecommendationsLambda`** recebe essa lista de IDs.
7.  *(Opcional, mas recomendado)* O Lambda faz uma consulta ao **Supabase** para "hidratar" esses IDs, buscando títulos e imagens (ex: "Zelda: BOTW", "Fight Club").
8.  O Lambda retorna a lista completa de recomendações para o **Cliente**.

-----

## 💻 Resumo da Stack

| Componente | Tecnologia | Propósito |
| :--- | :--- | :--- |
| **API Gateway** | AWS API Gateway | Roteamento e Front-door da API |
| **Autenticação** | AWS Cognito | Gerenciamento de usuários e JWT |
| **Compute (Core)** | AWS Lambda | Lógica de negócios (Python) |
| **Compute (ML)** | EC2 / Render | Processamento do modelo de recomendações |
| **Banco (User)** | AWS DynamoDB | Dados do usuário (listas, notas) |
| **Banco (Mídia)** | Supabase (PostgreSQL) | Catálogo global de mídias |