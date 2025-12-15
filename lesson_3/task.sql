/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
SELECT
    c.name AS category_name,
    COUNT(fc.film_id) AS film_count
FROM category c
JOIN film_category fc ON c.category_id = fc.category_id
GROUP BY c.name
ORDER BY film_count DESC;


/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/

-- v1 (using limit)
SELECT
    a.actor_id,
    a.first_name,
    a.last_name,
    COUNT(r.rental_id) AS rental_count
FROM actor a
JOIN film_actor fa ON a.actor_id = fa.actor_id
JOIN film f ON fa.film_id = f.film_id
JOIN inventory i ON f.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
GROUP BY a.actor_id, a.first_name, a.last_name
ORDER BY rental_count DESC
LIMIT 10;

-- v2 (using CTEs and rank in case og the same rentals amount for different actors)
WITH actor_rentals AS (
-- calculates rentals amount for each actor (with added rank for future filtering)
    SELECT
        a.actor_id,
        a.first_name,
        a.last_name,
        COUNT(r.rental_id) AS rental_count,
        RANK() OVER (ORDER BY COUNT(r.rental_id) DESC) AS rank
    FROM actor a
    JOIN film_actor fa ON a.actor_id = fa.actor_id
    JOIN film f ON fa.film_id = f.film_id
    JOIN inventory i ON f.film_id = i.film_id
    JOIN rental r ON i.inventory_id = r.inventory_id
    GROUP BY a.actor_id, a.first_name, a.last_name
)
SELECT
    actor_id,
    first_name,
    last_name,
    rental_count
FROM actor_rentals
WHERE rank <= 10
ORDER BY rental_count DESC, last_name, first_name;


/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/

-- v1 (using limit)
SELECT
    c.name AS category_name,
    SUM(p.amount) AS total_revenue
FROM category c
JOIN film_category fc ON c.category_id = fc.category_id
JOIN film f ON fc.film_id = f.film_id
JOIN inventory i ON f.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
JOIN payment p ON r.rental_id = p.rental_id
GROUP BY c.name
ORDER BY total_revenue DESC
LIMIT 1;

-- v2 (using CTEs and rank in case of the same revenue values for different categories)
WITH category_revenue AS (
-- calculates revenue for each category with rank
    SELECT
        c.name AS category_name,
        SUM(p.amount) AS total_revenue,
        RANK() OVER (ORDER BY SUM(p.amount) DESC) AS rank
    FROM category c
    JOIN film_category fc ON c.category_id = fc.category_id
    JOIN film f ON fc.film_id = f.film_id
    JOIN inventory i ON f.film_id = i.film_id
    JOIN rental r ON i.inventory_id = r.inventory_id
    JOIN payment p ON r.rental_id = p.rental_id
    GROUP BY c.name
)
SELECT
    category_name,
    total_revenue
FROM category_revenue
WHERE rank = 1
ORDER BY category_name;


/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/

SELECT f.title
FROM film f
LEFT JOIN inventory i ON f.film_id = i.film_id
WHERE i.inventory_id IS NULL;


/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/

-- v1 (using limit)
SELECT
    a.actor_id,
    a.first_name,
    a.last_name,
    COUNT(fa.film_id) AS film_count
FROM actor a
JOIN film_actor fa ON a.actor_id = fa.actor_id
JOIN film_category fc ON fa.film_id = fc.film_id
JOIN category c ON fc.category_id = c.category_id
WHERE c.name = 'Children'
GROUP BY a.actor_id, a.first_name, a.last_name
ORDER BY film_count DESC
LIMIT 3;

-- v2 (using CTEs and rank)
WITH children_actors AS (
-- Calculates amount of films for each actor in Children category with rank
    SELECT
        a.actor_id,
        a.first_name,
        a.last_name,
        COUNT(fa.film_id) AS film_count,
        RANK() OVER (ORDER BY COUNT(fa.film_id) DESC) AS rank
    FROM actor a
    JOIN film_actor fa ON a.actor_id = fa.actor_id
    JOIN film_category fc ON fa.film_id = fc.film_id
    JOIN category c ON fc.category_id = c.category_id
    WHERE c.name = 'Children'
    GROUP BY a.actor_id, a.first_name, a.last_name
)
SELECT
    actor_id,
    first_name,
    last_name,
    film_count
FROM children_actors
WHERE rank <= 3
ORDER BY last_name, first_name;