UPDATE midia
SET generos_unificados = ARRAY(
  SELECT DISTINCT unnest_super_genre
  FROM (
    SELECT
      CASE TRIM(g)
        -- AÇÃO
        WHEN 'Action' THEN ARRAY['Ação']
        WHEN 'Arcade' THEN ARRAY['Ação']
        WHEN 'Hack and slash/Beat ''em up' THEN ARRAY['Ação']
        WHEN 'Fighting' THEN ARRAY['Ação', 'Esportes / Competitivo']
        WHEN 'Shooter' THEN ARRAY['Ação']
        WHEN 'Pinball' THEN ARRAY['Ação']
        
        -- AVENTURA
        WHEN 'Adventure' THEN ARRAY['Aventura']
        WHEN 'Platform' THEN ARRAY['Ação', 'Aventura', 'Infantil / Família']
        WHEN 'Point-and-click' THEN ARRAY['Aventura', 'Estratégia / Raciocínio']
        WHEN 'Indie' THEN ARRAY['Aventura']
        WHEN 'Open world' THEN ARRAY['Aventura', 'Ação']
        WHEN 'Sandbox' THEN ARRAY['Aventura', 'Estratégia / Raciocínio']
        WHEN 'Survival' THEN ARRAY['Ação', 'Aventura', 'Terror / Suspense']
        
        -- RPG E FANTASIA
        WHEN 'Role-playing (RPG)' THEN ARRAY['Aventura', 'Fantasia', 'Estratégia / Raciocínio']
        WHEN 'Fantasy' THEN ARRAY['Fantasia']
        WHEN 'Science fiction' THEN ARRAY['Ficção Científica']
        
        -- ESTRATÉGIA
        WHEN 'Strategy' THEN ARRAY['Estratégia / Raciocínio']
        WHEN 'Real Time Strategy (RTS)' THEN ARRAY['Estratégia / Raciocínio', 'Ação']
        WHEN 'Turn-based strategy (TBS)' THEN ARRAY['Estratégia / Raciocínio']
        WHEN 'Tactical' THEN ARRAY['Estratégia / Raciocínio', 'Ação']
        WHEN '4X (explore, expand, exploit, and exterminate)' THEN ARRAY['Estratégia / Raciocínio']
        WHEN 'Card & Board Game' THEN ARRAY['Estratégia / Raciocínio']
        WHEN 'Puzzle' THEN ARRAY['Estratégia / Raciocínio']
        WHEN 'Quiz/Trivia' THEN ARRAY['Estratégia / Raciocínio', 'Realidade / Educação']
        WHEN 'Warfare' THEN ARRAY['Ação', 'Estratégia / Raciocínio']
        WHEN 'Stealth' THEN ARRAY['Ação', 'Estratégia / Raciocínio']
        
        -- COMPETITIVO E ESPORTES
        WHEN 'MOBA' THEN ARRAY['Esportes / Competitivo', 'Ação', 'Estratégia / Raciocínio']
        WHEN 'Sport' THEN ARRAY['Esportes / Competitivo']
        WHEN 'Racing' THEN ARRAY['Esportes / Competitivo', 'Ação']
        
        -- SIMULAÇÃO E REALIDADE
        WHEN 'Simulator' THEN ARRAY['Realidade / Educação', 'Estratégia / Raciocínio']
        WHEN 'Business' THEN ARRAY['Estratégia / Raciocínio', 'Realidade / Educação']
        WHEN 'Educational' THEN ARRAY['Realidade / Educação']
        WHEN 'Non-fiction' THEN ARRAY['Realidade / Educação']
        WHEN 'Historical' THEN ARRAY['Realidade / Educação', 'Drama']
        
        -- NARRATIVA E EMOÇÃO
        WHEN 'Visual Novel' THEN ARRAY['Drama', 'Romance', 'Aventura']
        WHEN 'Drama' THEN ARRAY['Drama']
        WHEN 'Romance' THEN ARRAY['Romance']
        WHEN 'Comedy' THEN ARRAY['Comédia']
        WHEN 'Music' THEN ARRAY['Música']
        
        -- TENSÃO
        WHEN 'Horror' THEN ARRAY['Terror / Suspense']
        WHEN 'Thriller' THEN ARRAY['Terror / Suspense']
        WHEN 'Mystery' THEN ARRAY['Terror / Suspense', 'Estratégia / Raciocínio']
        
        -- PÚBLICO ESPECÍFICO
        WHEN 'Kids' THEN ARRAY['Infantil / Família']
        WHEN 'Party' THEN ARRAY['Infantil / Família', 'Esportes / Competitivo']
        WHEN 'Erotic' THEN ARRAY['Adulto']
        
        ELSE ARRAY[]::text[]
      END as mapped_array
    FROM unnest(generos) as g
  ) sub_map,
  unnest(sub_map.mapped_array) as unnest_super_genre
)
WHERE categoria = 'jogo';