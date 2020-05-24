-- create view for students table
create view students_view as
select id,name
from students;

-- retrive rows from view
select * from students_view;