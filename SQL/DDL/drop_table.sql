-- delete table students(It will delete data and table structure(column,datatype)from the students table)

drop table students; -- table deleted,so there is no table existed in schema sql_practice

desc students;      -- if run this command there is an error says that table dosen't exist

-- create table students with in sql_pracrice schema

create table sql_practice.students 
( --   columnname 	datatype
		`Id` 		int 			NOT NULL,
		`Name` 		varchar(250) 	NOT NULL,
		`Gender` 	varchar(25) 	NOT NULL,
		`Age` 		int 			NOT NULL,
	     Primary key (`Id`) 
    -- a table can contain primary key, helps in joining tables.
);
