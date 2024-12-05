
-- SQL Project - Data Cleaning



SELECT * 
FROM world_layoffs.layoffs;


-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;


-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways



-- 1. Remove Duplicates

# First let's check for duplicates



SELECT *
FROM world_layoffs.layoffs_staging
;

SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		world_layoffs.layoffs_staging;



SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
-- let's just look at oda to confirm
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda'
;
-- it looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

-- these are our real duplicates 
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially

-- now you may want to write it like this:
WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE
;


WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging
)
DELETE FROM world_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;

-- one solution, which I think is a good one. Is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column
-- so let's do it!!

ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;


SELECT *
FROM world_layoffs.layoffs_staging
;

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

-- now that we have this we can delete rows were row_num is greater than 2

DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;

-- 2. Standardize Data

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- let's take a look at these
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';
-- nothing wrong here
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- now if we check those are all null

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls if possible

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- ---------------------------------------------------

-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- now that's taken care of:
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- --------------------------------------------------
-- we also need to look at 

SELECT *
FROM world_layoffs.layoffs_staging2;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;


-- Let's also fix the date columns:
SELECT *
FROM world_layoffs.layoffs_staging2;

-- we can use str to date to update this field
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM world_layoffs.layoffs_staging2;

-- 3. Look at Null Values
-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase
-- so there isn't anything I want to change with the null values
-- 4. remove any columns and rows we need to

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
SELECT * 
FROM world_layoffs.layoffs_staging2;

-- Another way of cleaning 
-- use manju;
-- show tables;
-- drop table t1,t3,t5,one,one1;
-- drop table excel_table1,excel_table;
-- create view cust as select  first_name , last_name from employee;
-- select * from cust;
-- create index ind 
-- on employee(first_name);
-- select * from ind;
-- show indexes from employee;
-- use employee_db;
-- use manju;
-- rename table ex to exa;
-- alter table employee
-- add column exa varchar(50);
-- insert into employee(exa) values("sjahs");
-- update employee set exa="ahsh" where emp_id=100;
-- select * from employee
-- select * from student_csv;
-- use manju;
-- use manju;
-- delimiter $$
-- create procedure emp()

-- begin 
-- 		select * from employee;
-- end $$

-- delimiter ;
-- call emp
-- delimiter $$
-- create procedure stu()

-- begin 
-- 		select * from student;
-- end $$

-- delimiter ;
-- call stu();
-- drop procedure stu;
-- delimiter $$
-- create procedure f_emp_id(in id int)

-- begin 
-- 		select * from employee 
--         where emp_id = id;
-- end $$
-- delimiter ;
-- call f_emp_id(102);
-- call emp();
-- update employee set sal = salary*1 where emp_id =100;
-- create trigger inc_sal
-- before update on employee 
-- for each row 
-- set new.salary = (new.salary +new.sal );
-- update employee 
-- set sal = 25000 where emp_id = 102;
-- drop trigger inc_sal;
-- call emp();
-- update employee set sal = 22000 where emp_id = 106;
-- CREATE TABLE ingredients AS
-- SELECT "chili" AS dish, "beans" AS part UNION
-- SELECT "chili" , "onions" UNION
-- SELECT "soup" , "broth" UNION
-- SELECT "soup" , "onions" UNION
-- SELECT "beans" , "beans";
-- select * from ingredients;
-- REATE TABLE shops AS
-- SELECT "beans" AS food, "A" AS shop, 2 AS price UNION
-- SELECT "beans" , "B" , 2 AS price UNION
-- SELECT "onions" , "A" , 3 UNION
-- SELECT "onions" , "B" , 2 UNION
-- SELECT "broth" , "A" , 3 UNION
-- SELECT "broth" , "B" , 5;
-- select * from shops;
-- select food, min(price) as lowest_price from shops group by food;
-- select i.dish,sum(s.price) as total_price from ingredients as i  join shops as s  where shop ="A" group by dish;
-- select food from shops group by food having count( distinct price  )>1  
-- use manju;
-- create table students(
-- student_id int primary key,
-- first_name varchar(50),
-- last_name varchar(50),
-- course_id int,
-- year_of_study int
-- );
-- alter table students
-- add foreign key (course_id) references courses(course_id);
-- describe students;
-- insert into students values (1,"John","Doe",	101	,1),(2,	"Jane",	"Smith",102,	2),(
-- 3	,"Alice	","Brown"	,101	,3),(4	,"Bob","	Johnson"	,103,	4),
-- (5,"	Carol","	Taylor",	102,	3);
-- drop table students;
-- select * from students;
-- use manju;
-- delimiter $$ 
-- call stu();
--         create procedure stu()
-- begin 
-- select * from student;
-- end $$ 

-- delimiter ;
-- drop procedure stu ;
-- DELIMITER $$

-- CREATE PROCEDURE stu()
-- BEGIN
--     SELECT * FROM students;
-- END $$
-- DELIMITER ;
-- call stu();
-- create table courses (
-- course_id int primary key ,
-- course_name varchar(100),
-- department varchar(50)
-- );
-- COURSES TABLE :--
-- insert into courses values (101	,"Data Structures","Computer Sci"),(102,	"Linear Algebra","	Mathematics"),(
-- 103,"	Physics I","	Physics"),(104	,"Chemistry	","Chemistry"),
-- (105,"	Operating Systems","	Computer S");
-- delimiter $$ 
-- create procedure cou()
-- begin 
-- select * from courses;
-- end $$
-- delimiter ;
-- call cou();
-- select concat(s.first_name," " ,s.last_name),c.course_name from students as s left join courses as c
-- on s.course_id=c.course_id union
-- select c.course_name from courses as c 
-- where c.course_id not in (select course_id from students);
-- SELECT CONCAT(s.first_name, ' ', s.last_name) AS student_name, c.course_name
-- FROM students AS s
-- LEFT JOIN courses AS c ON s.course_id = c.course_id

-- UNION all

-- SELECT NULL AS student_name, c.course_name
-- FROM courses AS c
-- WHERE c.course_id NOT IN (SELECT course_id FROM students);
-- Unique departments
-- SELECT DISTINCT department
-- FROM courses

-- UNION

-- -- Unique years of study
-- SELECT DISTINCT CAST(year_of_study AS CHAR) AS department
-- FROM students;

-- select department from courses
-- union 
-- select distinct cast( year_of_study as CHAR) as department from students;

-- select concat(s.first_name,s.last_name ) as student_name,c.course_name as course_names from students as s 
-- join courses as c on c.course_id = s.course_id 
-- union 
-- select NUll as first_name, c.course_name as course_name from
-- courses as c join students as s 
-- on c.course_id not in ( select course_id from students);

-- CREATE TABLE employees (
--     emp_id INT PRIMARY KEY,            -- Employee ID
--     emp_name VARCHAR(50),              -- Employee Name
--     manager_id INT,                    -- Manager ID (references emp_id of another employee)
--     department VARCHAR(50),            -- Department Name
--     salary DECIMAL(10, 2)              -- Employee Salary
-- );

--  Sample Data
-- INSERT INTO employees (emp_id, emp_name, manager_id, department,
--  salary) VALUES
-- (1, 'Alice', NULL, 'HR', 80000.00),   -- Alice is the head of HR
-- (2, 'Bob', 1, 'HR', 60000.00),        -- Bob reports to Alice
-- (3, 'Charlie', 1, 'HR', 60000.00),    -- Charlie reports to Alice
-- (4, 'David', 2, 'HR', 50000.00),      -- David reports to Bob
-- (5, 'Eve', 2, 'HR', 50000.00),        -- Eve reports to Bob
-- (6, 'Frank', NULL, 'Engineering', 90000.00), -- Frank is the head of Engineering
-- (7, 'Grace', 6, 'Engineering', 70000.00),   -- Grace reports to Frank
-- (8, 'Hank', 6, 'Engineering', 70000.00),    -- Hank reports to Frank
-- (9, 'Ivy', 7, 'Engineering', 60000.00);     

-- delimiter $$ 

-- create procedure emps()
-- begin 
-- select * from employees;
-- end $$
-- delimiter ;
-- select a.emp_name as emp_name , b.emp_name as ref_name from employees as a left join employees as b 
-- on b.manager_id = a.emp_id;

-- select emp_name  from employees where manager_id is null;

-- select a.emp_name , b.emp_name from employees as a join employees as b 
-- on b.manager_id = a.emp_id where a.manager_id is null ;

-- select emp_name   from employees where manager_id is null;
-- use manju;
-- delimiter $$ 

-- create procedure tab()
-- begin 
--     select * from `table`;
-- end $$
-- delimiter ;
-- call tab();

-- delimiter $$
-- create trigger che_co
-- before insert on courses 
-- for each row
-- if new.course_name = null then set new.course_name = "mech";
-- end if ; $$
-- drop trigger che_co;

-- delimiter $$
-- create trigger c_chk
-- before insert on courses 
-- for each row 
-- if new.department= "mech" then set new.department = "cse";
-- end if $$
-- call cou();
-- insert into courses values(11,"sansanb","mech");
-- delete from courses 
-- where course_id = 10;
-- call cou();
-- use manju;
-- CREATE TABLE employees3(
--     emp_id INT PRIMARY KEY,            -- Employee ID
--     emp_name VARCHAR(50),              -- Employee Name
--     department VARCHAR(50),            -- Department Name
--     salary DECIMAL(10, 2),             -- Employee Salary
--     last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Tracks updates
-- );

-- CREATE TABLE salary_audit (
--     audit_id INT AUTO_INCREMENT PRIMARY KEY,  -- Audit ID
--     emp_id INT,                               -- Employee ID
--     old_salary DECIMAL(10, 2),                -- Old Salary
--     new_salary DECIMAL(10, 2),                -- New Salary
--     changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Change Timestamp
-- );

--  Sample Data
-- INSERT INTO employees3 (emp_id, emp_name, department, salary) VALUES
-- (1, 'Alice', 'HR', 80000.00),
-- (2, 'Bob', 'HR', 60000.00),
-- (3, 'Charlie', 'Engineering', 70000.00),
-- (4, 'David', 'Engineering', 50000.00),
-- (5, 'Eve', 'Marketing', 75000.00);
--  Sample Data for salary_audit
-- INSERT INTO salary_audit (emp_id, old_salary, new_salary, changed_at) VALUES
-- (1, 75000.00, 80000.00, '2024-12-01 10:00:00'),
-- (2, 55000.00, 60000.00, '2024-12-01 11:00:00'),
-- (3, 65000.00, 70000.00, '2024-12-01 12:00:00');

-- delimiter $$ 

-- create procedure emp3()
-- begin 
-- select * from employees3;
-- end $$
-- delimiter 
-- delimiter $$ 
-- create procedure aud()
-- begin 
-- select * from salary_audit;
-- end $$
-- delimiter ;
-- delimiter $$
-- create trigger two
-- after update on employees3
-- for each row 
-- if old.salary != new.salary then 
-- insert into salary_audit(emp_id, old_salary,new_salary,changed_at)
-- values 
-- (old.emp_id,old.salary, new.salary,current_timestamp());
-- end if $$
-- update employees3
-- set salary = 80000 where emp_id=3;
-- call aud();
-- drop trigger two;
-- drop trigger one;
 -- delimiter $$ 
-- create trigger thr
-- before insert on employees
-- for each row
-- if new.salary <0 then
-- signal sqlstate '45000'
-- set message_text = "salary cannot be negative ";
-- end if $$

-- insert into employees3 values( 6,"manju", "nabs",-1002,current_timestamp()
-- );
-- call emp3;
 -- drop trigger thr;
-- delete from employees3 
-- where emp_id between 6 and 7;
-- delimiter $$
-- CREATE TRIGGER thr
-- before UPDATE ON employees
-- FOR EACH ROW

--     IF NEW.salary < 0 THEN
--         SIGNAL SQLSTATE '45000'
--         SET MESSAGE_TEXT = 'Salary cannot be negative';
-- end if $$

-- Q. Audit Employee Deletions: Create a trigger to log deleted
-- employee data into a new table deleted_employees.
 -- create table del_emp(
--  emp_id int primary key ,
--  emp_name varchar(50),
--  department varchar(50),
--  salary int,
--  deleted_time TIMESTAMP DEFAULT 
--  CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
-- );
-- delimiter // 
-- create procedure del()
-- begin 
-- select * from del_emp;
-- end //
-- delimiter ;
-- call del();
-- delimiter $$
-- create trigger four
-- after delete on employees3
-- for each row 
-- insert into del_emp values(old.emp_id,old.emp_name,
-- old.department,old.salary,current_timestamp());
-- $$
-- delete from employees3
-- where emp_id=6;
-- call del();

-- delimiter $$
-- create trigger five
-- after insert on employees3
-- for each row 
-- if new.department is null then 
-- update employees3
-- set department = "engineering " where emp_id=new.emp_id ;
-- end if $$
-- delimiter ;
-- insert into employees3 values( 8,"ansman",null, 19000.00,
-- current_timestamp());
-- call emp3();
-- delimiter $$ 
-- create trigger six
-- before insert on employees3
-- for each row 
-- if new.salary >100000 then set new.salary = 100000;
-- end if $$
-- insert into employees3 values(120,"sas","asajh",120000,current_timestamp());
-- call emp3;

-- create table tot_sal(
-- department varchar(50),
-- tot_sal int
-- );
-- delimiter $$
-- create trigger eig()
-- after insert on department
-- for each row 
-- insert into tot_sal values(new.department,(select sum(salary) 
-- from employees3 where department = new.department ))
-- end if $$
-- delimiter ;
-- delimiter $$ 
-- create trigger nine
-- before delete on employees3
-- for each row
-- if old.department = 'hr' then 
-- signal sqlstate '45000'
-- set message_text = "Deletion of HR employees is not allowed";
-- end if $$
-- delimiter ;
-- insert into employees3 values(9,"ans","hr",1000,current_timestamp());
-- delete from employees3
-- where department='hr';
-- use manju;
-- delimiter $$
-- create event eve1
-- on schedule every 30 second
-- do begin 
-- delete  from employee3
-- where emp_id=120;
-- end $$
-- delimiter ;
-- call emp3();
-- show variables like 'event%';

-- create database data_cleaning;
-- use data_cleaning;








