use sql_practice;

-- Below script is used to create student table in sql_practice schema

create table students 
( --   columnname 	datatype
		`Id` 		int 			NOT NULL,
		`Name` 		varchar(250) 	NOT NULL,
		`Gender` 	varchar(25) 	NOT NULL,
		`Age` 		int 			NOT NULL,
		Primary key (`Id`)
);

-- describe the students table
desc students;

-- check only columns in the student table
SELECT COLUMN_NAME
FROM Information_schema.COLUMNS
WHERE TABLE_NAME = 'students';




