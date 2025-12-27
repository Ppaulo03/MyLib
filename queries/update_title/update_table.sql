UPDATE midia
SET titulos_alternativos = temp.titulos_alternativos
FROM temp_titulos AS temp
WHERE 
    midia.titulo = temp.titulo 
    AND midia.ano_lancamento = temp.ano_lancamento
    AND midia.categoria = 'anime';

DROP TABLE temp_titulos;