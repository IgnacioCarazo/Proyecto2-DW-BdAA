--Q1: Calcula el tiempo promedio de juego por subgénero, separando múltiples géneros en la columna `name`.
SELECT 
    LTRIM(value) AS subgenre_name, -- Eliminamos espacios en blanco a la izquierda
    AVG(PF.playtime_forever) AS avg_playtime_per_subgenre
FROM 
    PlaytimeFact AS PF
LEFT JOIN 
    DimGenre AS DG ON PF.genre_id = DG.id
CROSS APPLY 
    STRING_SPLIT(DG.name, ',') -- Dividimos los géneros en valores individuales
GROUP BY 
    LTRIM(value) -- Agrupamos por subgénero limpio

--Q2: Calcula el tiempo promedio de juego por subcategoría, separando múltiples categorías en la columna `name`.
SELECT 
    LTRIM(value) AS subcategory_name, -- Eliminamos espacios en blanco a la izquierda
    AVG(PF.playtime_forever) AS avg_playtime_per_subcategory
FROM 
    PlaytimeFact AS PF
LEFT JOIN 
    DimCategory AS DC ON PF.category_id = DC.id
CROSS APPLY 
    STRING_SPLIT(DC.name, ',') -- Dividimos las categorías en valores individuales
GROUP BY 
    LTRIM(value) -- Agrupamos por subcategoría limpia

--Q3: Calcula el tiempo promedio de juego por desarrollador.
SELECT    
    DimDeveloper.name AS Developer, -- Nombre del desarrollador
    AVG(PlaytimeFact.playtime_forever) AS Average -- Tiempo promedio de juego
FROM    
    PlaytimeFact
INNER JOIN    
    DimDeveloper ON PlaytimeFact.developer_id = DimDeveloper.id
GROUP BY
    DimDeveloper.name

--Q4: Calcula el porcentaje de reseñas positivas y negativas por idioma.
SELECT 
    l.name AS [Language], -- Idioma de la reseña
    ROUND(CAST(SUM(CASE WHEN gf.recommended = 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(gf.recommended) * 100, 2) AS PositivePercentage, -- Porcentaje de reseñas positivas
    ROUND(CAST(SUM(CASE WHEN gf.recommended = 0 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(gf.recommended) * 100, 2) AS NegativePercentage -- Porcentaje de reseñas negativas
FROM 
    GameFact gf
INNER JOIN 
    DimLanguaje l ON gf.language_id = l.id
GROUP BY 
    l.name

--Q5: Obtiene estadísticas de votos en reseñas, como porcentaje de reseñas útiles y divertidas.
SELECT 
    COUNT(*) AS TotalReviews, -- Total de reseñas en la base de datos
    SUM(CASE WHEN votes_helpful > 0 THEN 1 ELSE 0 END) AS ReviewsWithHelpfulVotes, -- Reseñas con al menos 1 voto útil
    ROUND((SUM(CASE WHEN votes_helpful > 0 THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS PercentageHelpfulReviews, -- Porcentaje de reseñas útiles
    SUM(CASE WHEN votes_funny > 0 THEN 1 ELSE 0 END) AS ReviewsWithFunnyVotes, -- Reseñas con al menos 1 voto divertido
    ROUND((SUM(CASE WHEN votes_funny > 0 THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS PercentageFunnyReviews, -- Porcentaje de reseñas divertidas
    SUM(CASE WHEN votes_helpful > 0 AND votes_funny > 0 THEN 1 ELSE 0 END) AS ReviewsWithBothVotes, -- Reseñas con votos útiles y divertidos
    ROUND((SUM(CASE WHEN votes_helpful > 0 AND votes_funny > 0 THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS PercentageBothVotes -- Porcentaje de reseñas con ambos votos
FROM 
    GameFact

--Q6: Cuenta las reseñas de juegos según el método de adquisición (compra en Steam o recibido gratis).
SELECT    
    GameFact.game_id AS IdGame, -- ID del juego
    DimGame.name AS GameName, -- Nombre del juego
    COUNT(CASE WHEN DimMethod_of_Acquisition.steam_purchase = 1 AND DimMethod_of_Acquisition.received_for_free = 0 THEN 1 END) AS SteamPruchaseReviews, -- Reseñas de juegos comprados en Steam
    COUNT(CASE WHEN DimMethod_of_Acquisition.steam_purchase = 0 AND DimMethod_of_Acquisition.received_for_free = 1 THEN 1 END) AS ReceivedFreeReviews -- Reseñas de juegos recibidos gratis
FROM    
    PlaytimeFact
INNER JOIN GameFact ON PlaytimeFact.game_id = GameFact.game_id
INNER JOIN DimGame ON GameFact.game_id = DimGame.id
INNER JOIN DimMethod_of_Acquisition ON PlaytimeFact.method_of_acquisition_id = DimMethod_of_Acquisition.id
GROUP BY
    GameFact.game_id, DimGame.name

--Q7: Obtiene los 10 juegos con mayor porcentaje jugado después de una reseña y su porcentaje de recomendación.
SELECT TOP 10
    g.id,
    g.name AS GameName, -- Nombre del juego
    p.AvgPercentagePlayedAfterReview, -- Promedio de porcentaje jugado después de la reseña
    r.RecommendationPercentage -- Porcentaje de recomendación del juego
FROM 
    DimGame g
LEFT JOIN (
    SELECT 
        game_id,
        ROUND(AVG(percentage_played_after_review), 2) AS AvgPercentagePlayedAfterReview
    FROM 
        PlaytimeFact
    GROUP BY 
        game_id
) p ON g.id = p.game_id
LEFT JOIN (
    SELECT 
        game_id,
        ROUND((SUM(CASE WHEN recommended = 1 THEN 1 ELSE 0 END) * 100.0) / NULLIF(COUNT(recommended), 0), 2) AS RecommendationPercentage
    FROM 
        GameFact
    GROUP BY 
        game_id
) r ON g.id = r.game_id
WHERE 
    p.AvgPercentagePlayedAfterReview IS NOT NULL -- Solo juegos con datos

--Q8: Cuenta el total de reseñas por año.
SELECT
    DimDate.year AS YearReview, -- Año de la reseña
    COUNT(GameFact.author_id) AS TotalReviews -- Total de reseñas en ese año
FROM    
    GameFact
INNER JOIN DimDate ON GameFact.date_id = DimDate.id
GROUP BY
    DimDate.year

--Q9: Obtiene el juego más reseñado por año.
SELECT 
    T.year,
    T.name,
    T.TotalReviews
FROM (
    SELECT  
        DimDate.year, -- Año de la reseña
        DimGame.name, -- Nombre del juego
        COUNT(GameFact.author_id) AS TotalReviews, -- Total de reseñas
        RANK() OVER (PARTITION BY DimDate.year ORDER BY COUNT(GameFact.author_id) DESC) AS Rank -- Ranking dentro del año
    FROM GameFact
    INNER JOIN DimDate ON GameFact.date_id = DimDate.id
    INNER JOIN DimGame ON GameFact.game_id = DimGame.id
    GROUP BY DimDate.year, DimGame.name
) AS T
WHERE T.Rank = 1 -- Solo el juego más reseñado por año

--Q10: Obtiene el total de reseñas mensuales del juego "Among Us" en el año 2020.
SELECT    
    DimGame.name,
    DimDate.year AS YearReview, -- Año de la reseña
    DimDate.month AS MonthReview, -- Mes de la reseña
    COUNT(GameFact.author_id) AS TotalReviews -- Total de reseñas en ese mes
FROM    
    GameFact
INNER JOIN DimDate ON GameFact.date_id = DimDate.id
INNER JOIN DimGame ON GameFact.game_id = DimGame.id
WHERE 
    DimGame.name = 'Among Us' AND DimDate.year = 2020
GROUP BY
    DimGame.name, DimDate.year, DimDate.month

--Q11: Calcula el porcentaje de recomendación y total de reseñas del juego "Among Us" en 2020.
SELECT    
    DimGame.name,
    DimDate.year,
    ROUND((COUNT(CASE WHEN GameFact.recommended = 1 THEN 1 END) * 100.0 / COUNT(GameFact.recommended)),2) AS RecommendationPercentaje, -- Porcentaje de recomendaciones
    COUNT(GameFact.review_id) AS TotalReviews -- Total de reseñas
FROM    
    GameFact
INNER JOIN DimGame ON GameFact.game_id = DimGame.id
INNER JOIN DimDate ON GameFact.date_id = DimDate.id
WHERE 
    DimGame.name = 'Among Us' AND DimDate.year = 2020
GROUP BY
    DimGame.name, DimDate.year
