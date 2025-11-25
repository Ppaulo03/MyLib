# MyLib — Arquitetura de Dados

O **MyLib** é um aplicativo inspirado em plataformas como *MyAnimeList* e *Letterboxd*, permitindo que usuários cataloguem e avaliem diversos tipos de mídia — como filmes, séries, jogos, animes, mangás etc.

A arquitetura de dados é composta por dois sistemas:

  * **DynamoDB** → armazena dados do usuário e suas relações com conteúdos (ratings, listas, favoritos etc).
  * **Supabase** → armazena datasets de mídia estruturados (catálogo oficial de filmes, séries, jogos, etc).

-----

## 📦 DynamoDB (User Data Layer)

O DynamoDB é usado como o banco orientado ao usuário, seguindo um **modelo multitenant** usando *Partition Key (PK)* e *Sort Key (SK)*.

### Tabelas

#### **1. Users**

Armazena informações essenciais de autenticação e metadados do usuário.

| Campo | Tipo | Descrição |
| :--- | :--- | :--- |
| `user_id` | string (PK) | ID único do usuário |
| `email` | string | Email para login |
| `password_hash` | string | Hash da senha (ou referência se usar provider externo) |
| `created_at` | ISO date | Data de criação |
| `last_login` | ISO date | Último login |

-----

#### **2. Dados**

Tabela principal multitenant contendo todos os dados associados ao usuário.

**Chaves**

  * **PK → `user_id`**
  * **SK → tipo de objeto + identificador**

-----

### 🧱 Estrutura da SK

#### **1. Perfil do Usuário**

Armazena dados complementares do perfil.

  * **PK:** `user_id`
  * **SK:** `perfil`

**Exemplo de item:**

```json
{
  "user_id": "123",
  "sk": "perfil",
  "username": "joaozin",
  "bio": "Amante de jogos e animes",
  "avatar_url": "https://..."
}
```

#### **2. Itens de Mídia Consumidos / Avaliados**

Cada mídia adicionada pelo usuário segue o padrão:

  * **PK:** `user_id`
  * **SK:** `item#{categoria}#{conteudo_id}`

Onde:

  * `categoria` → filme, serie, anime, jogo, etc.
  * `conteudo_id` → ID do item no Supabase, garantindo consistência com o dataset central.

**Exemplo:**

  * `item#jogo#987321`
  * `item#anime#001244`
  * `item#filme#550`

**Exemplo de item completo:**

```json
{
  "user_id": "123",
  "sk": "item#anime#001244",
  "status": "completed",
  "rating": 8,
  "progress": 24,
  "updated_at": "2025-01-01T12:00:00Z"
}
```

-----

## 🗃️ Supabase (Content Dataset Layer)

O Supabase armazena os datasets globais e estruturados de mídia, como uma espécie de “catálogo oficial”.

**Importante:**

  * Cada entrada de mídia possui um ID estável, usado na SK do DynamoDB.
  * Permite consultas eficientes e normalizadas (ex.: gêneros, estúdios, franquias, plataformas).

**Exemplo de tabela de mídia:**

**Tabela: `medias`**

| Campo | Tipo | Descrição |
| :--- | :--- | :--- |
| `id` | bigint (PK) | ID do conteúdo (usado no DynamoDB) |
| `categoria` | text | Tipo (anime, jogo, filme...) |
| `titulo` | text | Nome da mídia |
| `descricao` | text | Sinopse |
| `ano_lancamento` | int | Ano de lançamento |
| `metadata` | jsonb | Dados extras |

-----

## 🔗 Relação DynamoDB ↔ Supabase

O DynamoDB não duplica os dados da mídia.

  * O **DynamoDB** guarda APENAS dados do usuário (ratings, progresso, favoritos).
  * O **Supabase** guarda os dados fixos e globais da mídia.

A ligação é feita pelo `conteudo_id`, que é o mesmo nas duas bases.

**Fluxo típico:**

1.  Usuário adiciona algo à lista → DynamoDB cria item: `item#categoria#id`.
2.  App usa o `id` para puxar informações detalhadas da mídia no Supabase.
3.  Interface combina (join no app) dados do usuário + dados globais.

-----

## 📚 Exemplo de Estrutura Completa

```
DynamoDB (Dados)
└── user_id: "123"
    ├── sk: "perfil"
    ├── sk: "item#anime#001244"
    ├── sk: "item#jogo#987321"
    └── sk: "item#filme#550"

Supabase (Medias)
├── id: 001244, categoria: anime, titulo: "Naruto"
├── id: 987321, categoria: jogo, titulo: "Zelda: BOTW"
└── id: 550, categoria: filme, titulo: "Fight Club"
```