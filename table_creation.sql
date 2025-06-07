-- by range
CREATE TABLE users_range (
  id INT,
  name VARCHAR(100),
  age INT,
  country_code CHAR(2)
)
PARTITION BY RANGE (age)
  (STARTING FROM (0) ENDING (25) INCLUSIVE,
   STARTING FROM (26) ENDING (50) INCLUSIVE,
   STARTING FROM (51) ENDING (150) INCLUSIVE);

-- by list (simulated by range)
CREATE TABLE users_list (
  id INT,
  name VARCHAR(100),
  age INT,
  country_code CHAR(2)
)
PARTITION BY RANGE (country_code)
  (STARTING FROM 'UY' ENDING 'UY' INCLUSIVE ,
   STARTING FROM 'AR' ENDING 'AR' INCLUSIVE ,
   STARTING FROM 'BR' ENDING 'BR' INCLUSIVE);

CREATE TABLE users (
  id INT,
  name VARCHAR(100),
  age INT,
  country_code CHAR(2)
);