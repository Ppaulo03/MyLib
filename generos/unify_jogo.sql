UPDATE midia
SET generos_unificados = ARRAY(
  SELECT DISTINCT unnest_super_genre
  FROM (
    SELECT
      CASE TRIM(g)
        -- AÇÃO PURA
        WHEN 'Ação' THEN ARRAY['Ação']
        WHEN 'Arcade' THEN ARRAY['Ação']
        WHEN 'Beat ''em up' THEN ARRAY['Ação']
        WHEN 'Luta' THEN ARRAY['Ação', 'Esportes / Competitivo']
        WHEN 'Tiro' THEN ARRAY['Ação']
        WHEN 'Pinball' THEN ARRAY['Ação']
        
        -- AVENTURA E MUNDOS
        WHEN 'Aventura' THEN ARRAY['Aventura']
        WHEN 'Plataforma' THEN ARRAY['Ação', 'Aventura', 'Infantil / Família']
        WHEN 'Point-and-Click' THEN ARRAY['Aventura', 'Estratégia / Raciocínio']
        WHEN 'Indie' THEN ARRAY['Aventura'] 
        
        -- RPG
        WHEN 'RPG' THEN ARRAY['Aventura', 'Fantasia', 'Estratégia / Raciocínio']
        
        -- ESTRATÉGIA
        WHEN 'Estratégia' THEN ARRAY['Estratégia / Raciocínio']
        WHEN 'Estratégia em Tempo Real' THEN ARRAY['Estratégia / Raciocínio', 'Ação']
        WHEN 'Estratégia por Turnos' THEN ARRAY['Estratégia / Raciocínio']
        WHEN 'Tático' THEN ARRAY['Estratégia / Raciocínio']
        WHEN 'Cartas e Tabuleiro' THEN ARRAY['Estratégia / Raciocínio']
        WHEN 'Quebra-Cabeça' THEN ARRAY['Estratégia / Raciocínio']
        WHEN 'Quiz/Trivia' THEN ARRAY['Estratégia / Raciocínio', 'Realidade / Educação']
        
        -- COMPETITIVO
        WHEN 'MOBA' THEN ARRAY['Esportes / Competitivo', 'Ação', 'Estratégia / Raciocínio']
        WHEN 'Esporte' THEN ARRAY['Esportes / Competitivo']
        WHEN 'Corrida' THEN ARRAY['Esportes / Competitivo', 'Ação']
        
        -- NARRATIVA
        WHEN 'Visual Novel' THEN ARRAY['Drama', 'Romance', 'Aventura']
        WHEN 'Simulador' THEN ARRAY['Realidade / Educação', 'Estratégia / Raciocínio']
        WHEN 'Música' THEN ARRAY['Música', 'Ação']
        
        ELSE ARRAY[]::text[]
      END as mapped_array
    FROM unnest(generos) as g
  ) sub_map,
  unnest(sub_map.mapped_array) as unnest_super_genre
)
WHERE categoria = 'jogo';