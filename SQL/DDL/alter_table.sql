--  ALTER is one of the DDL is used to add,drop or modify columns in the exsisting table.

alter table students add      -- add email_id and address columns  to the exixsting student table
(email_id varchar(50),
 address varchar(30))
;                   		     -- columns created
desc students;

--  drop the column email_id and address from the student table
alter table students 
drop column email_id, 
drop column address ;

-- modified column name datatype from varchar(250) to varchar (200)
alter table students modify name varchar(200);

