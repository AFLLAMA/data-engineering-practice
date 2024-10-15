/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...
select c.name as category_naem, count(f.film_id) as films
from film f
join film_category fc on fc.film_id=f.film_id
join category c on c.category_id=fc.category_id
group by c.category_id
order by films desc;

/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...
select a.first_name || ' ' || a.last_name as actor, count(r.rental_id) as rentals
from actor a
join film_actor fa on fa.actor_id = a.actor_id
join film f on f.film_id = fa.film_id
join inventory i on f.film_id = i.film_id
join rental r on r.inventory_id = i.inventory_id
group by a.actor_id
order by rentals desc
limit 10;

/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
-- SQL code goes here...
select c.name as category_name, sum(p.amount) as amount_total
from film f
join film_category fc on fc.film_id=f.film_id
join category c on c.category_id=fc.category_id
join inventory i on f.film_id = i.film_id
join rental r on r.inventory_id = i.inventory_id
join payment p on p.rental_id = r.rental_id
group by c.category_id
order by amount_total desc
limit 1;

/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
-- SQL code goes here...
select fi.title as film_name
from film fi
left join inventory i on fi.film_id = i.film_id
where i.film_id is null;

/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
-- SQL code goes here...
select a.first_name || ' ' || a.last_name as actor, count(c.category_id) as children_genre_casts
from actor a
join film_actor fa on fa.actor_id = a.actor_id
join film f on f.film_id = fa.film_id
join film_category fc on fc.film_id=f.film_id
join category c on c.category_id=fc.category_id
where c.name = 'Children'
group by a.actor_id
order by children_genre_casts desc
limit 3;
